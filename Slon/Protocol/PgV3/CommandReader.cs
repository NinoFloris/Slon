using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Types;
using Slon.Protocol.Pg;
using Slon.Protocol.PgV3.Descriptors;

namespace Slon.Protocol.PgV3;

enum CommandReaderState
{
    None = default,
    Initialized,
    Active,
    Completed,
    UnrecoverablyCompleted
}

/// <summary>
/// Specifies the type of SQL statement, e.g. SELECT
/// </summary>
enum StatementType
{
#pragma warning disable 1591
    Unknown = default,
    Select,
    Insert,
    Delete,
    Update,
    CreateTableAs,
    Move,
    Fetch,
    Copy,
    Other,
    Merge,
    Call
#pragma warning restore 1591
}

class PgV3CommandReader
{
    readonly Action<PgV3CommandReader>? _returnAction;
    CommandReaderState _state;

    // Recycled instances.
    readonly CommandStart _commandStart; // Quite big so it's a class.
    DataRowReader _rowReader; // Mutable struct, don't make readonly.
    CommandComplete _commandComplete; // Mutable struct, don't make readonly.
    // Set during InitializeAsync.
    Operation _operation;

    public PgV3CommandReader(Encoding encoding, Action<PgV3CommandReader>? returnAction = null)
    {
        _returnAction = returnAction;
        _commandStart = new(new RowDescription(initialCapacity: 10, encoding));
        _commandComplete = new();
    }

    public CommandReaderState State => _state;
    public int FieldCount => ThrowIfNotInitialized()._commandStart.Fields.Length;
    public bool HasRows => ThrowIfNotInitialized()._rowReader.ResumptionData.IsDefault;

    public StatementType StatementType => ThrowIfNotCompleted()._commandComplete.StatementType;
    public Oid Oid => ThrowIfNotCompleted()._commandComplete.Oid;

    public ulong? RowsRetrieved
    {
        get
        {
            ThrowIfNotCompleted();
            if (RowsAffected.HasValue)
                return null;

            return _commandComplete.Rows;
        }
    }
    public ulong? RowsAffected
    {
        get
        {
            ThrowIfNotCompleted();
            switch (StatementType)
            {
                case StatementType.Update:
                case StatementType.Insert:
                case StatementType.Delete:
                case StatementType.Copy:
                case StatementType.Move:
                case StatementType.Merge:
                    return _commandComplete.Rows;
                default:
                    return null;
            }
        }
    }

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
    // If this throws due to a protocol issue it will complete the operation and transition itself to CompleteUnrecoverably.
    // The operation itself will be completed with an exception if the known protocol state is indeterminate
    // or it will be completed 'succesfully' if the next operation could still succeed due to being able to reach a safe point.
    public async ValueTask InitializeAsync(CommandContext<CommandExecution> commandContext, CancellationToken cancellationToken = default)
    {
        if (_state is not CommandReaderState.None)
            ThrowNotReset();

        var opTask = commandContext.GetOperation();
        if (!opTask.IsCompleted)
            ThrowOpTaskNotReady();

        if (opTask.Result.IsCompleted)
            ThrowOpCompleted();

        var operation = opTask.Result;
        var commandStart = _commandStart;
        commandStart.Initialize(commandContext);
        // We cannot read ExecutionFlags before ReadMessageAsync (see CommandStart.Read for more details), so we can't split the execution paths at this point.
        try
        {
            commandStart = await ReadMessageAsync(commandStart, cancellationToken, operation).ConfigureAwait(false);
            if (commandStart.Error is { } error)
            {
                if (commandStart is { PlanError: not null, Statement: not null })
                    commandStart.Statement.Invalidate();
                ThrowPostgresException(error);
            }
            if (commandStart.ExecutionFlags.HasPreparing())
                CompletePreparation(commandStart);
        }
        catch(Exception ex)
        {
            if (commandStart.ExecutionFlags.HasPreparing())
            {
                // This goes in reverse order, each earlier check implies the later checks/stages were successful.
                if (commandStart.IsDescribeCompleted)
                    // We didn't get to the end but we did get enough info to complete preparation.
                    CompletePreparation(commandStart);
                else if (commandStart.IsParseCompleted)
                    // At this point the statement itself is prepared from a pg perspective, we need to close it.
                    CloseStatement(commandContext, commandStart);
                else
                    // Nothing happened yet, we'll just cancel the preparation attempt.
                    commandStart.Session!.CancelPreparation(ex);
            }

            CompleteUnrecoverably(operation, ex, abortProtocol: !commandStart.IsReadyForNext);
            throw;
        }

        if (commandStart.TryGetCompleteResult(out var result))
        {
            _state = CommandReaderState.Initialized;
            Complete(result.CommandComplete);
            return;
        }

        _operation = operation;
        _rowReader = new(commandStart.Buffer, commandStart.ResumptionData, commandStart.Fields);
        _state = CommandReaderState.Initialized;

        static void ThrowPostgresException(ErrorOrNoticeMessage error) => throw new PostgresException(error);
        static void ThrowOpTaskNotReady() => throw new ArgumentException("Operation task on given command context is not ready yet.", nameof(commandContext));
        static void ThrowOpCompleted() => throw new ArgumentException("Operation on given command context is already completed.", nameof(commandContext));
        static void ThrowNotReset() => throw new InvalidOperationException("CommandReader was not reset.");

        static void CloseStatement(CommandContext<CommandExecution> commandContext, CommandStart commandStart)
        {
            var protocol = (PgV3Protocol)commandContext.GetOperation().Result.Protocol;
            var statement = (PgV3Statement)commandStart.Session!.Statement!;
            protocol.CloseStatement(statement);
        }

        static void CompletePreparation(CommandStart commandStart)
        {
            var statement = (PgV3Statement)commandStart.Session!.Statement!;
            if (!statement.IsComplete)
                statement.AddFields(ImmutableArray.Create(commandStart.Fields.Span));
            // We always call complete even if the statement was already complete, this is to allow any per connection bookkeeping to take place.
            commandStart.Session.CompletePreparation(statement);
        }
    }


    // TODO
    // public void Close()
    // {
    //     while (Read())
    //     {}
    //     CloseCore();
    // }

    public async ValueTask CloseAsync()
    {
        while (await ReadAsync().ConfigureAwait(false))
        {}
    }

    ValueTask<T> ReadMessageAsync<T>(T message, CancellationToken cancellationToken, Operation? operation = null) where T : IBackendMessage<PgV3Header>
    {
        var op = operation ?? _operation;
        return Core(this, (PgV3Protocol)op.Protocol, op, message, cancellationToken);

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        static async ValueTask<T> Core(PgV3CommandReader instance, PgV3Protocol protocol, Operation operation, T message, CancellationToken cancellationToken)
        {
            try
            {
                return await protocol.ReadMessageAsync(message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not TimeoutException && (ex is not OperationCanceledException || ex is OperationCanceledException oce && oce.CancellationToken != cancellationToken))
            {
                instance.CompleteUnrecoverably(operation, ex);
                throw;
            }
        }
    }

    PgV3CommandReader ThrowIfNotInitialized()
    {
        if (_state is CommandReaderState.None)
            ThrowNotInitialized();

        return this;

        void ThrowNotInitialized() => throw new InvalidOperationException("Command reader wasn't initialized properly, this is a bug.");
    }

    PgV3CommandReader ThrowIfNotCompleted()
    {
        if (_state is not CommandReaderState.Completed)
            ThrowNotCompleted();

        return this;

        void ThrowNotCompleted() => throw new InvalidOperationException("Command reader is not successfully completed.");
    }

    ValueTask HandleWritableParameters(bool async, IReadOnlyCollection<IParameterSession> writableParameters)
    {
        // TODO actually handle writable intput/output and output parameters.
        _commandStart.Session?.CloseWritableParameters();
        throw new NotImplementedException();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        // If we have writable parameters we immediately go async so we can fully read and process the first row.
        if (_state is CommandReaderState.Initialized && _commandStart.Session is { WritableParameters: { } writableParameters })
            return Core(this, default, writableParameters, cancellationToken);

        switch (_state)
        {
            case CommandReaderState.Initialized:
                // First row is already loaded.
                _state = CommandReaderState.Active;
                return Task.FromResult(true);
            case CommandReaderState.Active:
                ReadStatus status;
                // TODO benchmark adding a try catch by default again (last time it impacted perf quite a bit).
                if (BackendMessage.DebugEnabled)
                {
                    try
                    {
                        if (_rowReader.ReadNext(out status))
                            return Task.FromResult(true);
                    }
                    catch (Exception ex)
                    {
                        CompleteUnrecoverably(_operation, ex);
                        throw;
                    }
                }
                else if (_rowReader.ReadNext(out status))
                    return Task.FromResult(true);

                return status switch
                {
                    // TODO implement ConsumeData
                    // Only go async once we have to
                    ReadStatus.NeedMoreData or ReadStatus.AsyncResponse => Core(this, status, null, cancellationToken),
                    ReadStatus.Done or ReadStatus.InvalidData => CompleteCommand(this, unexpected: status is ReadStatus.InvalidData, cancellationToken).AsTask(),
                    _ => ThrowArgumentOutOfRange()
                };
            default:
                return HandleUncommon(this);
        }

        static async Task<bool> Core(PgV3CommandReader instance, ReadStatus status, IReadOnlyCollection<IParameterSession>? writableParameters = null, CancellationToken cancellationToken = default)
        {
            // Skip the read if we haven't gotten any writable parameters, in that case we handle the given status (which is the most common reason for calling this method).
            var skipRead = writableParameters is null;
            // Only expect writableParameters if we're on the first row (CommandReaderState.Initialized).
            DebugShim.Assert(writableParameters is null || instance._state is CommandReaderState.Initialized);
            switch (instance._state)
            {
                case CommandReaderState.Initialized:
                    // First row is already loaded.
                    instance._state = CommandReaderState.Active;
                    if (writableParameters is not null)
                        await instance.HandleWritableParameters(async: true, writableParameters);
                    return true;
                case CommandReaderState.Active:
                    while (true)
                    {
                        if (BackendMessage.DebugEnabled)
                        {
                            try
                            {
                                if (!skipRead && instance._rowReader.ReadNext(out status))
                                    return true;
                            }
                            catch (Exception ex)
                            {
                                instance.CompleteUnrecoverably(instance._operation, ex);
                                throw;
                            }
                        }
                        else if (!skipRead && instance._rowReader.ReadNext(out status))
                            return true;
                        skipRead = false;

                        switch (status)
                        {
                            // TODO implement ConsumeData
                            case ReadStatus.NeedMoreData:
                                await BufferData(instance, cancellationToken).ConfigureAwait(false);
                                break;
                            case ReadStatus.Done or ReadStatus.InvalidData:
                                return await CompleteCommand(instance, unexpected: status is ReadStatus.InvalidData, cancellationToken).ConfigureAwait(false);
                            case ReadStatus.AsyncResponse:
                                await HandleAsyncResponse(instance, cancellationToken).ConfigureAwait(false);
                                break;
                            default:
                                return await ThrowArgumentOutOfRange().ConfigureAwait(false);
                        }
                    }
                default:
                    return await HandleUncommon(instance).ConfigureAwait(false);
            }
        }

        static Task<bool> HandleUncommon(PgV3CommandReader instance)
        {
            switch (instance._state)
            {
                case CommandReaderState.Completed:
                case CommandReaderState.UnrecoverablyCompleted:
                    return Task.FromResult(false);
                case CommandReaderState.None:
                    instance.ThrowIfNotInitialized();
                    return Task.FromResult(false);
                default:
                {
                    var ex = new ArgumentOutOfRangeException();
                    instance.CompleteUnrecoverably(instance._operation, ex);
                    return Task.FromException<bool>(ex);
                }
            }
        }

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
        static async ValueTask BufferData(PgV3CommandReader instance, CancellationToken cancellationToken = default)
        {
            var result = await instance.ReadMessageAsync(
                new ExpandBuffer(instance._rowReader.ResumptionData, instance._rowReader.Consumed),
                cancellationToken).ConfigureAwait(false);
            instance._rowReader.ExpandBuffer(result.Buffer);
        }

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        static async ValueTask<bool> CompleteCommand(PgV3CommandReader instance, bool unexpected, CancellationToken cancellationToken = default)
        {
            if (unexpected)
            {
                // We don't need to pass an exception, InvalidData kills the connection.
                instance.CompleteUnrecoverably(instance._operation);
                return false;
            }

            var result = await instance.ReadMessageAsync(
                new CompleteCommand(instance._rowReader.ResumptionData, instance._rowReader.Consumed, instance._commandStart.Flags.HasErrorBarrier()),
                cancellationToken).ConfigureAwait(false);
            instance.Complete(result.CommandComplete);
            return false;
        }

        static ValueTask HandleAsyncResponse(PgV3CommandReader instance, CancellationToken cancellationToken = default)
        {
            // TODO implement async response, even though technically the protocol as implemented in postgres never mixes async responses and rows (see pg docs).
            throw new NotImplementedException();
        }

        static Task<bool> ThrowArgumentOutOfRange() => Task.FromException<bool>(new ArgumentOutOfRangeException());
    }

    void CompleteUnrecoverably(Operation operation, Exception? ex = null, bool abortProtocol = true)
    {
        if (_commandStart.Session is { } session)
            session.CloseWritableParameters();

        _state = CommandReaderState.UnrecoverablyCompleted;
        // We sometimes don't have to abort if we were able to advance the protocol to a safe point (RFQ).
        operation.Complete(abortProtocol ? ex : null);
    }

    void Complete(CommandComplete commandComplete)
    {
        _state = CommandReaderState.Completed;
        _commandComplete = commandComplete;
        _operation.Complete();
    }

    public void Reset()
    {
        _state = CommandReaderState.None;
        _commandStart.Reset();
        _rowReader = default;
        _returnAction?.Invoke(this);
    }

    struct ExpandBuffer : IPgV3BackendMessage
    {
        readonly MessageReader<PgV3Header>.ResumptionData _resumptionData;
        readonly long _consumed;
        bool _resumed;

        public ExpandBuffer(MessageReader<PgV3Header>.ResumptionData resumptionData, long consumed)
        {
            _resumptionData = resumptionData;
            _consumed = consumed;
        }

        public ReadOnlySequence<byte> Buffer { get; private set; }

        public ReadStatus Read(ref MessageReader<PgV3Header> reader)
        {
            if (_resumed)
            {
                // When we get resumed all we do is store the new buffer.
                Buffer = reader.Sequence;
                // Return a reader that has not consumed anything, this ensures no part of the buffer will be advanced out from under us.
                reader = MessageReader<PgV3Header>.Create(reader.Sequence);
                return ReadStatus.Done;
            }

            // Create a reader that has the right consumed state and header data for the outer read loop to figure out the next size.
            if (_resumptionData.IsDefault)
            {
                // We don't have enough data to read the next header, just advance to consumed.
                // The outer read loop will read at minimum a header length worth of new data before resuming.
                reader = MessageReader<PgV3Header>.Create(reader.Sequence);
                reader.Advance(_consumed);
            }
            else
                reader = _consumed == 0 ? MessageReader<PgV3Header>.Resume(reader.Sequence, _resumptionData) : MessageReader<PgV3Header>.Recreate(reader.Sequence, _resumptionData, _consumed);
            _resumed = true;
            return ReadStatus.NeedMoreData;
        }
    }

    class CommandStart: IPgV3BackendMessage
    {
        enum ReadState
        {
            Unstarted = default,
            Parse,
            Bind,
            Describe,
            RowDescription,
            RowOrCompletion,
            CommandComplete,
            Error,
            ErrorComplete
        }

        RowDescription _rowDescription;
        ReadState _state;
        MessageReader<PgV3Header>.ResumptionData _resumptionData;
        CompleteCommand _completeResult;
        CommandContext<CommandExecution> _commandContext;
        ErrorResponse _errorResponse;

        public ExecutionFlags ExecutionFlags { get; private set; }
        public CommandFlags Flags { get; private set; }
        public ICommandSession? Session { get; private set; }
        public PgV3Statement? Statement { get; private set; }
        public ReadOnlyMemory<StatementField> Fields { get; private set; } = ReadOnlyMemory<StatementField>.Empty;
        public ReadOnlySequence<byte> Buffer { get; private set; }
        public MessageReader<PgV3Header>.ResumptionData ResumptionData => _resumptionData;
        public bool IsParseCompleted => _state is not ReadState.Unstarted or ReadState.Parse;
        public bool IsDescribeCompleted => _state is ReadState.RowOrCompletion or ReadState.CommandComplete or ReadState.Error;
        public bool IsReadyForNext => _state is ReadState.CommandComplete or ReadState.ErrorComplete;

        public bool HasError => _state is not ReadState.RowOrCompletion or ReadState.CommandComplete;
        public ErrorOrNoticeMessage? Error => _errorResponse.Message;
        public ErrorOrNoticeMessage? PlanError => _errorResponse.Message is { SqlState: "0A000", Message: "cached plan must not change result type" } ? _errorResponse.Message : null;

        public void Initialize(in CommandContext<CommandExecution> commandContext)
        {
            _commandContext = commandContext;
        }

        void CreateErrorMessage()
        {
            _errorResponse = new ErrorResponse(_rowDescription.Encoding);
        }

        public bool TryGetCompleteResult(out CompleteCommand result)
        {
            if (_resumptionData.IsDefault)
            {
                result = _completeResult;
                return true;
            }

            result = default;
            return false;
        }

        public CommandStart(RowDescription rowDescription) => _rowDescription = rowDescription;

        public void Reset()
        {
            _rowDescription.Reset();
            Buffer = default;
            Fields = ReadOnlyMemory<StatementField>.Empty;
            _state = default;
            Statement = null;
            _errorResponse = default;
        }

        public ReadStatus Read(ref MessageReader<PgV3Header> reader)
        {
            var status = ReadStatus.InvalidData;
            if (_state is not ReadState.Error)
                status = ReadCore(ref reader);

            if (status is ReadStatus.InvalidData && reader.IsExpected(BackendCode.ErrorResponse, out _))
            {
                if (_state is not ReadState.Error)
                    CreateErrorMessage();

                // Make sure the error response can MoveNext once again.
                reader = MessageReader<PgV3Header>.Recreate(reader.Sequence, reader.GetResumptionData(), reader.Consumed);
                if (!reader.ReadMessage(ref _errorResponse, out status))
                {
                    _state = ReadState.Error;
                    return status;
                }

                // We swallow the error given we expect errors at this point in the protocol.
                return ReadStatus.Done;
            }

            return status;
        }

        ReadStatus ReadCore(ref MessageReader<PgV3Header> reader)
        {
            switch (_state)
            {
                case ReadState.Unstarted:
                    // We have to read ExecutionFlags and any Session/Statement *after* the socket read is done and this Read is called.
                    // As reads don't wait on writes (particularly true for multiplexing).
                    // Once we can read a response do we know for sure that the values are settled.
                    var commandExecution = _commandContext.GetCommandExecution();
                    (ExecutionFlags, Flags) = commandExecution.TryGetSessionOrStatement(out var commandSession, out var statement);
                    if (ExecutionFlags.HasPrepared())
                    {
                        // When prepared is set we should always have a statement, either alone or on a session.
                        var v3Statement = (PgV3Statement)(statement ?? commandSession?.Statement)!;
                        Statement = v3Statement;
                        Fields = v3Statement.Fields!.Value.AsMemory();
                        goto bind;
                    }
                    else if (commandSession is not null)
                    {
                        // Not every session has a statement, command sessions with just output sessions for instance.
                        Statement = (PgV3Statement?)commandSession.Statement;
                        Session = commandSession;
                    }

                    break;
                case ReadState.Parse:
                    goto parse;
                case ReadState.Bind:
                    goto bind;
                case ReadState.Describe:
                case ReadState.RowDescription:
                    goto describe;
                case ReadState.RowOrCompletion:
                case ReadState.CommandComplete:
                    goto rowOrCompletion;
            }

            parse:
            if (!reader.ReadMessage<ParseComplete>(out var status))
            {
                _state = ReadState.Parse;
                return status;
            }

            bind:
            if (!reader.ReadMessage<BindComplete>(out status))
            {
                _state = ReadState.Bind;
                return status;
            }

            if (ExecutionFlags.HasPrepared() || Statement?.IsComplete == true)
                goto rowOrCompletion;

            describe:
            // We take a copy to revert to once we know which of the two codes (NoData or RowDescription) we received.
            var readerCopy = reader;
            if (_state is not ReadState.RowDescription && !reader.MoveNext())
            {
                _state = ReadState.Describe;
                return ReadStatus.NeedMoreData;
            }

            switch (reader.Current.Code)
            {
                case BackendCode.NoData:
                    if (!reader.ConsumeCurrent())
                    {
                        _state = ReadState.Describe;
                        return ReadStatus.NeedMoreData;
                    }
                    break;
                default:
                    reader = readerCopy;
                    if (!reader.ReadMessage(ref _rowDescription, out status))
                    {
                        _state = ReadState.RowDescription;
                        return status;
                    }
                    Fields = _rowDescription.Fields;
                    break;
            }

            rowOrCompletion:
            if (_state is not ReadState.CommandComplete && !reader.MoveNext())
            {
                _state = ReadState.RowOrCompletion;
                return ReadStatus.NeedMoreData;
            }

            switch (reader.Current.Code)
            {
                case BackendCode.DataRow:
                    _resumptionData = reader.GetResumptionData();
                    Buffer = reader.UnconsumedSequence;
                    break;
                default:
                    if (_state is not ReadState.CommandComplete)
                        _completeResult = new CompleteCommand(reader.GetResumptionData(), reader.Consumed, Flags.HasErrorBarrier());

                    if (!reader.ReadMessage(ref _completeResult, out status))
                    {
                        _state = ReadState.CommandComplete;
                        return status;
                    }
                    break;
            }

            return ReadStatus.Done;
        }
    }

    struct CompleteCommand : IPgV3BackendMessage
    {
        readonly MessageReader<PgV3Header>.ResumptionData _resumptionData;
        readonly long _consumed;
        readonly bool _hasRfq;
        bool _atRfq;
        CommandComplete _commandComplete;
        ReadyForQuery _rfq;
        public CommandComplete CommandComplete => _commandComplete;
        public ReadyForQuery ReadyForQuery => _rfq;

        public CompleteCommand(MessageReader<PgV3Header>.ResumptionData resumptionData, long consumed, bool hasRfq)
        {
            _resumptionData = resumptionData;
            _consumed = consumed;
            _hasRfq = hasRfq;
        }

        public ReadStatus Read(ref MessageReader<PgV3Header> reader)
        {
            if (_atRfq)
                goto rfq;

            if (_resumptionData.Header.Code is BackendCode.EmptyQueryResponse)
            {
                if (!reader.ConsumeCurrent())
                    return ReadStatus.NeedMoreData;
            }
            else
            {
                reader = MessageReader<PgV3Header>.Recreate(reader.Sequence, _resumptionData, _consumed);
                if (!reader.ReadMessage(ref _commandComplete, out var ccStatus))
                    return ccStatus;
            }

            rfq:
            if (_hasRfq && !reader.ReadMessage(ref _rfq, out var status))
            {
                _atRfq = true;
                return status;
            }

            return ReadStatus.Done;
        }
    }

    struct DataRowReader
    {
        ReadOnlySequence<byte> _buffer;
        long _bufferLength;
        MessageReader<PgV3Header>.ResumptionData _resumptionData;
        readonly int _expectedFieldCount;
        long _consumed;
        bool _messageNeedsMoreData;

        long Remaining => _bufferLength - _consumed;
        public long Consumed => _consumed;
        public MessageReader<PgV3Header>.ResumptionData ResumptionData => _resumptionData;
        public ReadOnlySequence<byte> Buffer => _buffer;

        public DataRowReader(ReadOnlySequence<byte> buffer, MessageReader<PgV3Header>.ResumptionData resumptionData, ReadOnlyMemory<StatementField> fields)
        {
            _buffer = buffer;
            _bufferLength = buffer.Length;
            _resumptionData = resumptionData;
            _expectedFieldCount = fields.Length;
            _consumed = 0;
        }

        public void ExpandBuffer(ReadOnlySequence<byte> buffer)
        {
            _buffer = buffer;
            _bufferLength = buffer.Length;
        }

        // We're dropping down to manual here because reconstructing a SequenceReader every row is too slow.
        // Invariant is that this method cannot throw exceptions unless BackendMessage.DebugEnabled, who knew try catch could be expensive.
        // The reason we would want to know about any exceptions is that in such an event we must transition to CompleteUnrecoverably.
        public bool ReadNext(out ReadStatus status)
        {
            PgV3Header header;
            ReadOnlySequence<byte> buffer;
            if (_messageNeedsMoreData)
            {
                header = _resumptionData.Header;
                buffer = _buffer.Slice(_consumed);
                _messageNeedsMoreData = false;
            }
            else
            {
                if (!IsMessageBuffered(_resumptionData.Header.Length - _resumptionData.MessageIndex))
                {
                    status = ReadStatus.NeedMoreData;
                    return false;
                }

                _consumed += _resumptionData.Header.Length - _resumptionData.MessageIndex;

                buffer = _buffer.Slice(_consumed);
                if (!PgV3Header.TryParse(buffer, out header))
                {
                    if (!_messageNeedsMoreData)
                        _resumptionData = default;

                    status = ReadStatus.NeedMoreData;
                    return false;
                }
            }

            switch (header.Code)
            {
                case BackendCode.DataRow:
                    if (BackendMessage.DebugEnabled && !ReadRowDebug(buffer, header, out status))
                        return false;

                    if (Remaining < PgV3Header.ByteCount + sizeof(short))
                    {
                        _messageNeedsMoreData = true;
                        _resumptionData = new MessageReader<PgV3Header>.ResumptionData(header, 0);
                        status = ReadStatus.NeedMoreData;
                        return false;
                    }

                    _consumed += PgV3Header.ByteCount + sizeof(short);
                    _resumptionData = new MessageReader<PgV3Header>.ResumptionData(header, PgV3Header.ByteCount + sizeof(short));
                    status = ReadStatus.Done;
                    return true;
                default:
                    status = header.Code is BackendCode.CommandComplete ? ReadStatus.Done :
                        header.IsAsyncResponse ? ReadStatus.AsyncResponse : ReadStatus.InvalidData;

                    _consumed += PgV3Header.ByteCount;
                    _resumptionData = new MessageReader<PgV3Header>.ResumptionData(header, PgV3Header.ByteCount);
                    return false;
            }
        }

        bool ReadRowDebug(in ReadOnlySequence<byte> buffer, PgV3Header header, out ReadStatus status)
        {
            Span<byte> headerAndColumnCount = stackalloc byte[PgV3Header.ByteCount + sizeof(short)];
            if (!buffer.TryCopyTo(headerAndColumnCount))
            {
                _messageNeedsMoreData = true;
                _resumptionData = new MessageReader<PgV3Header>.ResumptionData(header, 0);
                status = ReadStatus.NeedMoreData;
                return false;
            }

            if (BinaryPrimitives.ReadInt16BigEndian(headerAndColumnCount.Slice(PgV3Header.ByteCount)) != _expectedFieldCount)
                throw new InvalidDataException("DataRow returned a different number of columns than was expected from the row description.");

            status = default;
            return true;
        }

        bool IsMessageBuffered(uint remainingMessage)
            => remainingMessage <= _bufferLength - _consumed;
    }
}

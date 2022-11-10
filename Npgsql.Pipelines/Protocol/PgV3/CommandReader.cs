using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;
using Npgsql.Pipelines.Pg.Types;
using Npgsql.Pipelines.Protocol.PgV3.Descriptors;

namespace Npgsql.Pipelines.Protocol.PgV3;

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

class CommandReader
{
    CommandReaderState _state;

    // Set during InitializeAsync.
    CommandContext _commandContext;

    // Recycled instances.
    readonly CommandStart _commandStart; // Quite big so it's a class.
    DataRowReader _rowReader; // Mutable struct, don't make readonly.
    CommandComplete _commandComplete; // Mutable struct, don't make readonly.

    public CommandReader()
    {
        _commandStart = new(new RowDescription(initialCapacity: 10));
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
    public async ValueTask InitializeAsync(CommandContext commandContext, CancellationToken cancellationToken = default)
    {
        if (_state is not CommandReaderState.None)
            ThrowNotReset();

        var opTask = commandContext.GetOperation();
        if (!opTask.IsCompleted)
            ThrowOpTaskNotCompleted();

        if (opTask.Result.IsCompleted)
            ThrowOpCompleted();

        _commandStart.Initialize(commandContext);
        var start = await ReadMessageAsync(_commandStart, cancellationToken, commandContext).ConfigureAwait(false);
        if (start.TryGetCompleteResult(out var result))
        {
            Complete(result.CommandComplete);
            return;
        }

        if (_commandStart.ExecutionFlags.HasPreparing())
        {
            DebugShim.Assert(_commandStart.Session is not null);
            DebugShim.Assert(_commandStart.Session.Statement is PgV3Statement);
            _commandStart.Session.CompletePreparation((PgV3Statement)_commandStart.Session.Statement with
            {
                Fields = ImmutableArray.Create(_commandStart.Fields.Span)
            });
        }

        _commandContext = commandContext;
        _rowReader = new(start.Buffer, start.ResumptionData, _commandStart.Fields);
        _state = CommandReaderState.Initialized;

        void ThrowOpTaskNotCompleted() => throw new ArgumentException("Operation task on given command context is not completed yet.", nameof(commandContext));
        void ThrowOpCompleted() => throw new ArgumentException("Operation on given command context is already completed.", nameof(commandContext));
        void ThrowNotReset() => throw new InvalidOperationException("CommandReader was not reset.");
    }


    ValueTask<T> ReadMessageAsync<T>(T message, CancellationToken cancellationToken, in CommandContext? commandContext = null) where T : IBackendMessage<PgV3Header>
    {
        // https://github.com/dotnet/roslyn/issues/65091
        if (!commandContext.HasValue)
            ThrowIfNotInitialized();

        var protocol = (PgV3Protocol)(commandContext ?? _commandContext).GetOperation().Result.Protocol;
        return Core(this, protocol, message, cancellationToken);

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        static async ValueTask<T> Core(CommandReader instance, PgV3Protocol protocol, T message, CancellationToken cancellationToken)
        {
            try
            {
                return await protocol.ReadMessageAsync(message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not TimeoutException && (ex is not OperationCanceledException || ex is OperationCanceledException oce && oce.CancellationToken != cancellationToken))
            {
                throw instance.Complete(null, ex);
            }
        }
    }

    CommandReader ThrowIfNotInitialized()
        => _state is CommandReaderState.None ? throw new InvalidOperationException("Command reader wasn't initialized properly, this is a bug.") : this;

    CommandReader ThrowIfNotCompleted()
        => _state != CommandReaderState.Completed ? throw new InvalidOperationException("Command reader is not successfully completed.") : this;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        switch (_state)
        {
            case CommandReaderState.Initialized:
                // First row is already loaded.
                _state = CommandReaderState.Active;
                return Task.FromResult(true);
            case CommandReaderState.Active:
                ReadStatus status;
                if (BackendMessage.DebugEnabled)
                {
                    try
                    {
                        if (_rowReader.ReadNext(out status))
                            return Task.FromResult(true);
                    }
                    catch (Exception ex)
                    {
                        throw Complete(null, ex);
                    }
                }
                else if (_rowReader.ReadNext(out status))
                    return Task.FromResult(true);

                return status switch
                {
                    // TODO implement ConsumeData
                    // Only go async once we have to
                    ReadStatus.NeedMoreData or ReadStatus.AsyncResponse => Core(this, status, cancellationToken),
                    ReadStatus.Done or ReadStatus.InvalidData => CompleteCommand(this, unexpected: status is ReadStatus.InvalidData, cancellationToken).AsTask(),
                    _ => ThrowArgumentOutOfRange()
                };
            default:
                return HandleUncommon(this);
        }

        static async Task<bool> Core(CommandReader instance, ReadStatus status, CancellationToken cancellationToken = default)
        {
            var first = true;
            switch (instance._state)
            {
                case CommandReaderState.Initialized:
                    // First row is already loaded.
                    instance._state = CommandReaderState.Active;
                    return true;
                case CommandReaderState.Active:
                    while (true)
                    {
                        if (BackendMessage.DebugEnabled)
                        {
                            try
                            {
                                if (!first && instance._rowReader.ReadNext(out status))
                                    return true;
                            }
                            catch (Exception ex)
                            {
                                throw instance.Complete(null, ex);
                            }
                        }
                        else if (instance._rowReader.ReadNext(out status))
                            return true;
                        first = false;

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

        static Task<bool> HandleUncommon(CommandReader instance)
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
                    return Task.FromException<bool>(instance.Complete(null, new ArgumentOutOfRangeException()));
            }
        }

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
        static async ValueTask BufferData(CommandReader instance, CancellationToken cancellationToken = default)
        {
            var result = await instance.ReadMessageAsync(new ExpandBuffer(instance._rowReader.ResumptionData, instance._rowReader.Consumed), cancellationToken).ConfigureAwait(false);
            instance._rowReader.ExpandBuffer(result.Buffer);
        }

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        static async ValueTask<bool> CompleteCommand(CommandReader instance, bool unexpected, CancellationToken cancellationToken = default)
        {
            if (unexpected)
            {
                // We don't need to pass an exception, InvalidData kills the connection.
                instance.Complete(null);
                return false;
            }

            var result =  await instance.ReadMessageAsync(new CompleteCommand(instance._rowReader.ResumptionData, instance._rowReader.Consumed, instance._commandStart.ExecutionFlags.HasErrorBarrier()), cancellationToken).ConfigureAwait(false);
            instance.Complete(result.CommandComplete);
            return false;
        }

        static ValueTask HandleAsyncResponse(CommandReader instance, CancellationToken cancellationToken = default)
        {
            // TODO implement async response, even though technically the protocol as implemented in postgres never mixes async responses and rows (see pg docs).
            throw new NotImplementedException();
        }

        static Task<bool> ThrowArgumentOutOfRange() => Task.FromException<bool>(new ArgumentOutOfRangeException());
    }

    [return: NotNullIfNotNull(nameof(ex))]
    Exception? Complete(CommandComplete? commandComplete, Exception? ex = null)
    {
        // https://github.com/dotnet/roslyn/issues/65091
        if (!commandComplete.HasValue)
        {
            _state = CommandReaderState.UnrecoverablyCompleted;
            _commandContext.GetOperation().Result.Complete(ex);
        }
        else
        {
            _state = CommandReaderState.Completed;
            _commandComplete = commandComplete.Value;
        }

        return ex;
    }

    public void Reset()
    {
        _state = CommandReaderState.None;
        _commandStart.Reset();
        _rowReader = default;
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
            CommandComplete
        }

        RowDescription _rowDescription;
        ReadState _state;
        MessageReader<PgV3Header>.ResumptionData _resumptionData;
        CompleteCommand _completeResult;
        CommandContext _commandContext;

        public ExecutionFlags ExecutionFlags { get; private set; }
        public ICommandSession? Session { get; private set; }
        public ReadOnlyMemory<StatementField> Fields { get; private set; } = ReadOnlyMemory<StatementField>.Empty;
        public ReadOnlySequence<byte> Buffer { get; private set; }
        public MessageReader<PgV3Header>.ResumptionData ResumptionData => _resumptionData;

        public void Initialize(in CommandContext commandContext)
        {
            _commandContext = commandContext;
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
        }

        public ReadStatus Read(ref MessageReader<PgV3Header> reader)
        {
            switch (_state)
            {
                case ReadState.Unstarted:
                    // We have to read ExecutionFlags and any Session/Statement *after* the socket read is done and this Read is called.
                    // As reads don't wait on writes (particularly true for multiplexing).
                    // Once we can read a response do we know for sure that the values are settled.
                    var commandExecution = _commandContext.GetCommandExecution();
                    ExecutionFlags = commandExecution.TryGetSessionOrStatement(out var session, out var statement);
                    if (ExecutionFlags.HasPrepared())
                    {
                        DebugShim.Assert(statement is PgV3Statement { IsComplete: true });
                        Fields = ((PgV3Statement)statement).Fields!.Value.AsMemory();
                        goto bind;
                    }
                    else if (ExecutionFlags.HasPreparing())
                        Session = session;

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

            if (ExecutionFlags.HasPrepared())
                goto rowOrCompletion;

            describe:
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
                        _completeResult = new CompleteCommand(reader.GetResumptionData(), reader.Consumed, ExecutionFlags.HasErrorBarrier());

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
        // The reason we'd want to know about any exceptions is that we should then transition to UnrecoverablyCompleted.
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

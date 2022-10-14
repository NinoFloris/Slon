using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines.Protocol;

enum CommandReaderState
{
    None = 0,
    Initialized,
    Completed,
    UnrecoverablyCompleted
}

/// <summary>
/// Specifies the type of SQL statement, e.g. SELECT
/// </summary>
public enum StatementType
{
#pragma warning disable 1591
    Unknown,
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
    readonly PgV3Protocol _protocol;
    RowDescription _rowDescription;
    CommandReaderState _commandReaderState;
    DataRowReader _rowReader;
    CommandComplete _commandComplete;

    public CommandReader(PgV3Protocol protocol)
    {
        _protocol = protocol;
    }

    public CommandReaderState State => _commandReaderState;
    public int FieldCount => ThrowIfNotInitialized()._rowDescription.Fields.Count;
    public bool HasRows => ThrowIfNotInitialized()._rowReader.ResumptionData.IsDefault;

    public StatementType StatementType => ThrowIfNotCompleted()._commandComplete.StatementType;
    public ulong RowsAffected => ThrowIfNotCompleted()._commandComplete.Rows;
    public Oid Oid => ThrowIfNotCompleted()._commandComplete.Oid;

    public async ValueTask InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (_commandReaderState == CommandReaderState.UnrecoverablyCompleted)
            throw new InvalidOperationException("Connection is broken.");

        try
        {
            var result = await _protocol.ReadMessageAsync<StartCommand>(cancellationToken).ConfigureAwait(false);
            if (result.IsCompleted)
            {
                Reset();
                Complete(result.CommandComplete);
                return;
            }

            _rowDescription = result.RowDescription;
            _rowReader = new DataRowReader(result.Buffer, result.ResumptionData, _rowDescription);
            _commandReaderState = CommandReaderState.Initialized;
        }
        catch (Exception ex) when (ex is not TimeoutException && (ex is not OperationCanceledException || ex is OperationCanceledException oce && oce.CancellationToken != cancellationToken))
        {
            Complete(null);
            throw;
        }
    }

    CommandReader ThrowIfNotInitialized()
    {
        if (_commandReaderState == CommandReaderState.None)
            throw new InvalidOperationException("Command reader wasn't initialized properly, this is a bug.");

        return this;
    }

    CommandReader ThrowIfNotCompleted()
    {
        if (_commandReaderState != CommandReaderState.Completed)
            throw new InvalidOperationException("Command reader is not completed yet.");

        return this;
    }

    public Task<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfNotInitialized();
        switch (_commandReaderState)
        {
            case CommandReaderState.Initialized:
                if(_rowReader.ReadNext(out var status))
                    return Task.FromResult(true);

                return status switch
                {
                    ReadStatus.NeedMoreData => BufferData(this, cancellationToken).AsTask(),
                    ReadStatus.Done => CompleteCommand(this, true, cancellationToken).AsTask(),
                    ReadStatus.InvalidData => CompleteCommand(this, false, cancellationToken).AsTask(),
                    ReadStatus.AsyncResponse => HandleAsyncResponse(this, cancellationToken).AsTask(),
                    _ => ThrowArgumentOutOfRange()
                };
            default:
                return Task.FromResult(false);
        }

        static async ValueTask<bool> BufferData(CommandReader instance, CancellationToken cancellationToken = default)
        {
            var result = await instance._protocol.ReadMessageAsync(new ExpandBuffer(instance._rowReader.ResumptionData, instance._rowReader.Consumed), cancellationToken).ConfigureAwait(false);
            instance._rowReader.ExpandBuffer(result.Buffer);
            return await instance.ReadAsync().ConfigureAwait(false);
        }

        static async ValueTask<bool> CompleteCommand(CommandReader instance, bool expected, CancellationToken cancellationToken = default)
        {
            if (!expected)
            {
                instance.Complete(null);
                return false;
            }

            var result =  await instance._protocol.ReadMessageAsync(new CompleteCommand(instance._rowReader.ResumptionData, instance._rowReader.Consumed), cancellationToken).ConfigureAwait(false);
            instance.Complete(result.CommandComplete);
            return false;
        }

        static ValueTask<bool> HandleAsyncResponse(CommandReader instance, CancellationToken cancellationToken = default)
        {
            // TODO implement async response.
            throw new NotImplementedException();
        }

        static Task<bool> ThrowArgumentOutOfRange() => throw new ArgumentOutOfRangeException();
    }

    void Complete(CommandComplete? commandComplete)
    {
        _commandReaderState = commandComplete.HasValue ? CommandReaderState.Completed : CommandReaderState.UnrecoverablyCompleted;
        if (commandComplete is not null)
            _commandComplete = commandComplete.Value;
    }

    void Reset()
    {
        _rowReader = default;
        _commandReaderState = CommandReaderState.None;
        _rowDescription = default;
    }

    struct ExpandBuffer : IBackendMessage
    {
        readonly MessageReader.ResumptionData _resumptionData;
        readonly long _consumed;
        bool _resumed;

        public ExpandBuffer(MessageReader.ResumptionData resumptionData, long consumed)
        {
            _resumptionData = resumptionData;
            _consumed = consumed;
        }

        public ReadOnlySequence<byte> Buffer { get; private set; }

        public ReadStatus Read(ref MessageReader reader)
        {
            if (_resumed)
            {
                // When we get resumed all we do is store the new buffer.
                Buffer = reader.Sequence;
                // Return a reader that has not consumed anything, this ensures no part of the buffer will be advanced out from under us.
                reader = MessageReader.Create(reader.Sequence);
                return ReadStatus.Done;
            }

            // Create a reader that has the right consumed state and header data for the outer read loop to figure out the next size.
            reader = _consumed == 0 ? MessageReader.Resume(reader.Sequence, _resumptionData) : MessageReader.Create(reader.Sequence, _resumptionData, _consumed);
            _resumed = true;
            return ReadStatus.NeedMoreData;
        }
    }

    struct StartCommand: IBackendMessage
    {
        enum ReadState
        {
            Parse = default,
            Bind,
            Describe,
            RowOrCompletion,
            Completion
        }

        ReadState _state;
        public ReadOnlySequence<byte> Buffer { get; private set; }
        public MessageReader.ResumptionData ResumptionData { get; private set; }
        public RowDescription RowDescription { get; private set; }
        public CommandComplete CommandComplete { get; private set; }
        public bool IsCompleted => _state == ReadState.Completion;

        public ReadStatus Read(ref MessageReader reader)
        {
            switch (_state)
            {
                case ReadState.Bind: goto bind;
                case ReadState.Describe: goto describe;
                case ReadState.RowOrCompletion: goto rowOrCompletion;
            }

            if (!reader.ReadMessage(new ParseComplete(), out var status))
                return status;

            _state = ReadState.Bind;
            bind:
            if (!reader.ReadMessage(new BindComplete(), out status))
                return status;

            _state = ReadState.Describe;
            describe:
            // Peeking with a copy only works if we're already at the end of the current message.
            Debug.Assert(reader.CurrentRemaining == 0);
            var readerCopy = reader;
            if (!reader.MoveNext())
                return ReadStatus.NeedMoreData;

            switch (reader.Current.Code)
            {
                case BackendCode.NoData:
                    if (!reader.ConsumeCurrent())
                        return ReadStatus.NeedMoreData;
                    break;
                default:
                    // TODO pool.
                    reader = readerCopy;
                    if (!reader.ReadMessage(RowDescription = new RowDescription(pool: null), out status))
                        return status;
                    break;
            }

            _state = ReadState.RowOrCompletion;
            rowOrCompletion:
            // Peeking with a copy only works if we're already at the end of the current message.
            Debug.Assert(reader.CurrentRemaining == 0);
            // readerCopy = reader;
            if (!reader.MoveNext())
                return ReadStatus.NeedMoreData;

            switch (reader.Current.Code)
            {
                case BackendCode.EmptyQueryResponse:
                    _state = ReadState.Completion;
                    if (!reader.ConsumeCurrent())
                        return ReadStatus.NeedMoreData;
                    break;
                case BackendCode.CommandComplete:
                    _state = ReadState.Completion;
                    // reader = readerCopy;
                    // // TODO a bit of a hack to let CommandComplete MoveNext again without doing another peek on the 'hot path'.
                    reader = MessageReader.Create(reader.Sequence, reader.GetResumptionData(), reader.Consumed);
                    if (!reader.ReadMessage(CommandComplete = new CommandComplete(), out status))
                        return status;
                    break;
                default:
                    if (!reader.IsExpected(BackendCode.DataRow, out status))
                        return status;
                    ResumptionData = reader.GetResumptionData();
                    // Explicitly rewind the sequence to mark everything the row reader could want to use as unconsumed.
                    reader.Rewind(reader.CurrentConsumed);
                    Buffer = reader.Sequence.Slice(reader.Consumed);
                    break;
            }

            return ReadStatus.Done;
        }
    }

    struct CompleteCommand : IBackendMessage
    {
        readonly MessageReader.ResumptionData _resumptionData;
        readonly long _consumed;
        public CommandComplete CommandComplete { get; private set; }

        public CompleteCommand(MessageReader.ResumptionData resumptionData, long consumed)
        {
            _resumptionData = resumptionData;
            _consumed = consumed;
        }

        public ReadStatus Read(ref MessageReader reader)
        {
            reader = MessageReader.Create(reader.Sequence, _resumptionData, _consumed);
            var commandComplete = new CommandComplete();
            var status = commandComplete.Read(ref reader);
            if (status != ReadStatus.Done)
                return status;

            CommandComplete = commandComplete;
            return ReadStatus.Done;
        }
    }

    struct DataRowReader
    {
        ReadOnlySequence<byte> _buffer;
        long _bufferLength;
        MessageReader.ResumptionData _resumptionData;
        readonly int _expectedColumnCount;
        long _consumed;

        public long Consumed => _consumed;
        public MessageReader.ResumptionData ResumptionData => _resumptionData;

        public DataRowReader(ReadOnlySequence<byte> buffer, MessageReader.ResumptionData resumptionData, RowDescription rowDescription)
        {
            _buffer = buffer;
            _bufferLength = buffer.Length;
            _resumptionData = resumptionData;
            _expectedColumnCount = rowDescription.Fields.Count;
            _consumed = resumptionData.MessageIndex;
        }

        public void ExpandBuffer(ReadOnlySequence<byte> buffer)
        {
            _buffer = buffer;
            _bufferLength = buffer.Length;
        }

        // We're dropping down to manual here because reconstructing a SequenceReader every row is too slow.
        public bool ReadNext(out ReadStatus status)
        {
            if (!IsMessageBuffered(_resumptionData.Header.Length - _resumptionData.MessageIndex))
            {
                status = ReadStatus.NeedMoreData;
                return false;
            }

            _consumed += _resumptionData.Header.Length - _resumptionData.MessageIndex;
            _resumptionData = new MessageReader.ResumptionData(_resumptionData.Header, _resumptionData.Header.Length);

            var buffer = _buffer.Slice(_consumed);
            if (!MessageHeader.TryParse(ref buffer, out var header))
            {
                status = ReadStatus.NeedMoreData;
                return false;
            }

            _consumed += MessageHeader.ByteCount;
            _resumptionData = new MessageReader.ResumptionData(header, MessageHeader.ByteCount);

            switch (header.Code)
            {
                case BackendCode.DataRow:
                    // TODO Read column count and validate against row description columns in backend debug mode.
                    status = ReadStatus.Done;
                    return true;
                case BackendCode.CommandComplete:
                    // This will be handled in the command reader.
                    status = ReadStatus.Done;
                    return false;
                default:
                    status = header.Code.IsAsyncResponse() ? ReadStatus.AsyncResponse : ReadStatus.InvalidData;
                    return false;
            }
        }

        bool IsMessageBuffered(uint remainingMessage)
            => remainingMessage <= _bufferLength - _consumed;
    }
}

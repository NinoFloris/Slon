using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;
using Npgsql.Pipelines.Protocol.PgV3;

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
    PgV3Protocol _protocol;
    RowDescription _rowDescription;
    CommandReaderState _commandReaderState;
    DataRowReader _rowReader;
    CommandComplete _commandComplete;

    public CommandReaderState State => _commandReaderState;
    public int FieldCount => ThrowIfNotInitialized()._rowDescription.Fields.Count;
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

    public async ValueTask InitializeAsync(PgProtocol protocol, CancellationToken cancellationToken = default)
    {
        _protocol = (PgV3Protocol)protocol;

        try
        {
            var result = await _protocol.ReadMessageAsync(new StartCommand(_protocol.GetRowDescription()), cancellationToken).ConfigureAwait(false);
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
            throw new InvalidOperationException("Command reader is not successfully completed.");

        return this;
    }

#if !NETSTANDARD2_0
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        switch (_commandReaderState)
        {
            case CommandReaderState.Initialized:
                if(_rowReader.ReadNext(out var status))
                    return new ValueTask<bool>(true);

                return status switch
                {
                    ReadStatus.NeedMoreData => BufferData(this, cancellationToken),
                    ReadStatus.Done => CompleteCommand(this, unexpected: false, cancellationToken),
                    ReadStatus.InvalidData => CompleteCommand(this, unexpected: true, cancellationToken),
                    ReadStatus.AsyncResponse => HandleAsyncResponse(this, cancellationToken),
                    _ => ThrowArgumentOutOfRange()
                };
            default:
                ThrowIfNotInitialized();
                return new ValueTask<bool>(false);
        }

        static async ValueTask<bool> BufferData(CommandReader instance, CancellationToken cancellationToken = default)
        {
            try
            {
                var result = await instance._protocol.ReadMessageAsync(new ExpandBuffer(instance._rowReader.ResumptionData, instance._rowReader.Consumed), cancellationToken).ConfigureAwait(false);
                instance._rowReader.ExpandBuffer(result.Buffer);
                return await instance.ReadAsync().ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not TimeoutException && (ex is not OperationCanceledException || ex is OperationCanceledException oce && oce.CancellationToken != cancellationToken))
            {
                instance.Complete(null);
                throw;
            }
        }

        static async ValueTask<bool> CompleteCommand(CommandReader instance, bool unexpected, CancellationToken cancellationToken = default)
        {
            if (unexpected)
            {
                instance.Complete(null);
                return false;
            }

            try
            {
                var result =  await instance._protocol.ReadMessageAsync(new CompleteCommand(instance._rowReader.ResumptionData, instance._rowReader.Consumed), cancellationToken).ConfigureAwait(false);
                instance.Complete(result.CommandComplete);
                return false;
            }
            catch (Exception ex) when (ex is not TimeoutException && (ex is not OperationCanceledException || ex is OperationCanceledException oce && oce.CancellationToken != cancellationToken))
            {
                instance.Complete(null);
                throw;
            }
        }

        static ValueTask<bool> HandleAsyncResponse(CommandReader instance, CancellationToken cancellationToken = default)
        {
            // TODO implement async response.
            throw new NotImplementedException();
        }

        static ValueTask<bool> ThrowArgumentOutOfRange() => throw new ArgumentOutOfRangeException();
    }

    void Complete(CommandComplete? commandComplete)
    {
        _commandReaderState = commandComplete.HasValue ? CommandReaderState.Completed : CommandReaderState.UnrecoverablyCompleted;
        if (commandComplete is not null)
            _commandComplete = commandComplete.Value;
    }

    public void Reset()
    {
        _commandReaderState = CommandReaderState.None;
        _rowDescription.Reset();
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
            reader = _consumed == 0 ? MessageReader<PgV3Header>.Resume(reader.Sequence, _resumptionData) : MessageReader<PgV3Header>.Create(reader.Sequence, _resumptionData, _consumed);
            _resumed = true;
            return ReadStatus.NeedMoreData;
        }
    }

    struct StartCommand: IPgV3BackendMessage
    {
        enum ReadState
        {
            Parse = default,
            Bind,
            Describe,
            RowDescription,
            RowOrCompletion,
            CommandComplete
        }

        ReadState _state;
        MessageReader<PgV3Header>.ResumptionData _resumptionData;
        public ReadOnlySequence<byte> Buffer { get; private set; }
        public MessageReader<PgV3Header>.ResumptionData ResumptionData => _resumptionData;
        public RowDescription RowDescription { get; private set; }
        public CommandComplete CommandComplete { get; private set; }
        public bool IsCompleted => _resumptionData.IsDefault;

        public StartCommand(RowDescription rowDescription)
        {
            RowDescription = rowDescription;
        }

        public ReadStatus Read(ref MessageReader<PgV3Header> reader)
        {
            switch (_state)
            {
                case ReadState.Bind:
                    goto bind;
                case ReadState.Describe:
                case ReadState.RowDescription:
                    goto describe;
                case ReadState.RowOrCompletion:
                case ReadState.CommandComplete:
                    goto rowOrCompletion;
            }

            if (!reader.ReadMessage<ParseComplete>(out var status))
                return status;

            bind:
            if (!reader.ReadMessage<BindComplete>(out status))
            {
                _state = ReadState.Bind;
                return status;
            }

            describe:
            var readerCopy = reader;
            if (_state != ReadState.RowDescription)
            {
                // Peeking with a copy only works if we're already at the end of the current message.
                Debug.Assert(!reader.HasCurrent || reader.CurrentRemaining == 0);
                if (!reader.MoveNext())
                {
                    _state = ReadState.Describe;
                    return ReadStatus.NeedMoreData;
                }
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
                    if (!reader.ReadMessage(RowDescription, out status))
                    {
                        _state = ReadState.RowDescription;
                        return status;
                    }
                    break;
            }

            rowOrCompletion:
            readerCopy = reader;
            if (_state != ReadState.CommandComplete)
            {
                // Peeking with a copy only works if we're already at the end of the current message.
                Debug.Assert(!reader.HasCurrent || reader.CurrentRemaining == 0);
                if (!reader.MoveNext())
                {
                    _state = ReadState.RowOrCompletion;
                    return ReadStatus.NeedMoreData;
                }
            }

            switch (reader.Current.Code)
            {
                case BackendCode.EmptyQueryResponse:
                    if (!reader.ConsumeCurrent())
                    {
                        _state = ReadState.RowOrCompletion;
                        return ReadStatus.NeedMoreData;
                    }
                    break;
                case BackendCode.CommandComplete:
                    reader = readerCopy;
                    if (!reader.ReadMessage(CommandComplete = new CommandComplete(), out status))
                    {
                        _state = ReadState.CommandComplete;
                        return status;
                    }
                    break;
                default:
                    if (!reader.IsExpected(BackendCode.DataRow, out status))
                        return status;
                    _resumptionData = reader.GetResumptionData();
                    // Explicitly reset the sequence to mark everything the row reader could want to use as unconsumed.
                    reader = readerCopy;
                    Buffer = reader.UnconsumedSequence;
                    break;
            }

            return ReadStatus.Done;
        }
    }

    struct CompleteCommand : IPgV3BackendMessage
    {
        readonly MessageReader<PgV3Header>.ResumptionData _resumptionData;
        readonly long _consumed;
        public CommandComplete CommandComplete { get; private set; }

        public CompleteCommand(MessageReader<PgV3Header>.ResumptionData resumptionData, long consumed)
        {
            _resumptionData = resumptionData;
            _consumed = consumed;
        }

        public ReadStatus Read(ref MessageReader<PgV3Header> reader)
        {
            reader = MessageReader<PgV3Header>.Create(reader.Sequence, _resumptionData, _consumed);
            if (!reader.ReadMessage(CommandComplete = new CommandComplete(), out var status))
                return status;

            return ReadStatus.Done;
        }
    }

    struct DataRowReader
    {
        ReadOnlySequence<byte> _buffer;
        long _bufferLength;
        MessageReader<PgV3Header>.ResumptionData _resumptionData;
        readonly int _expectedColumnCount;
        long _consumed;
        bool _messageNeedsMoreData;

        public long Consumed => _consumed;
        public MessageReader<PgV3Header>.ResumptionData ResumptionData => _resumptionData;

        public DataRowReader(ReadOnlySequence<byte> buffer, MessageReader<PgV3Header>.ResumptionData resumptionData, RowDescription rowDescription)
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
            PgV3Header header;
            ReadOnlySequence<byte> buffer;
            if (_messageNeedsMoreData)
            {
                header = _resumptionData.Header;
                buffer = _buffer.Slice(_consumed);
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
                        _resumptionData = _resumptionData with { MessageIndex = _resumptionData.Header.Length };

                    status = ReadStatus.NeedMoreData;
                    return false;
                }
            }

            switch (header.Code)
            {
                case BackendCode.DataRow:
                    if (BackendMessage.DebugEnabled && !ReadRowDebug(buffer, header, out status))
                        return false;

                    if (buffer.Length < PgV3Header.ByteCount + sizeof(short))
                    {
                        _messageNeedsMoreData = true;
                        _resumptionData = new MessageReader<PgV3Header>.ResumptionData(header, 0);
                        status = ReadStatus.NeedMoreData;
                        return false;
                    }

                    _consumed += PgV3Header.ByteCount + sizeof(short);
                    _resumptionData = new MessageReader<PgV3Header>.ResumptionData(header, PgV3Header.ByteCount + sizeof(short));
                    _messageNeedsMoreData = false;
                    status = ReadStatus.Done;
                    return true;
                default:
                    status = header.Code == BackendCode.CommandComplete ? ReadStatus.Done :
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

            if (BinaryPrimitives.ReadInt16BigEndian(headerAndColumnCount.Slice(PgV3Header.ByteCount)) != _expectedColumnCount)
                throw new InvalidDataException("DataRow returned a different number of columns than was expected from the row description.");

            status = default;
            return true;
        }

        bool IsMessageBuffered(uint remainingMessage)
            => remainingMessage <= _bufferLength - _consumed;
    }
}

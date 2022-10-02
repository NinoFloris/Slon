using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.QueryMessages;

namespace Npgsql.Pipelines;

enum CommandReaderState
{
    None = 0,
    Initialized,
    Completed,
    UnrecoverablyCompleted
}

class CommandReader
{
    readonly PgV3Protocol _protocol;
    RowDescription _rowDescription;
    CommandReaderState _commandReaderState;
    DataRowReader _rowReader;

    public CommandReader(PgV3Protocol protocol)
    {
        _protocol = protocol;
    }

    public CommandReaderState State => _commandReaderState;

    public async ValueTask InitializeAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await _protocol.ReadMessageAsync<StartCommand>(cancellationToken).ConfigureAwait(false);
            if (result.CommandComplete) // TODO fully implement command complete (rows affected etc)
            {
                await CompleteAsync().ConfigureAwait(false);
                return;
            }

            _rowDescription = result.RowDescription;
            _rowReader = new DataRowReader(result.Buffer, result.ResumptionData, _rowDescription);
            _commandReaderState = CommandReaderState.Initialized;
        }
        catch (Exception ex) when (ex is not TimeoutException && (ex is not OperationCanceledException || ex is OperationCanceledException oce && oce.CancellationToken != cancellationToken))
        {
            await CompleteAsync(false).ConfigureAwait(false);
            throw;
        }
    }

    public Task<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        switch (_commandReaderState)
        {
            case CommandReaderState.Initialized:
                if(_rowReader.ReadNext(out var status))
                    return Task.FromResult(true);

                switch (status)
                {
                    case ReadStatus.NeedMoreData:
                        return BufferAsync(this, cancellationToken);
                    case ReadStatus.Done:
                        return CompleteAsync();
                    default:
                        throw new InvalidOperationException("Unexpected status from row reader, this is a bug.");
                }

            case CommandReaderState.Completed:
            case CommandReaderState.UnrecoverablyCompleted:
                return Task.FromResult(false);
            default:
                throw new InvalidOperationException("Command reader wasn't initialized properly, this is a bug.");
        }

        static async Task<bool> BufferAsync(CommandReader instance, CancellationToken cancellationToken = default)
        {
            var result = await instance._protocol.ReadMessageAsync(new ExpandBuffer(instance._rowReader.ResumptionData, instance._rowReader.Consumed), cancellationToken).ConfigureAwait(false);
            instance._rowReader.ExpandBuffer(result.Buffer);
            return await instance.ReadAsync().ConfigureAwait(false);
        }
    }

    async Task<bool> CompleteAsync(bool expected = true)
    {
        if (expected && _commandReaderState == CommandReaderState.Initialized)
            await _protocol.ReadMessageAsync(new CompleteCommand(_rowReader.Consumed)).ConfigureAwait(false);

        _commandReaderState = expected ? CommandReaderState.Completed : CommandReaderState.UnrecoverablyCompleted;

        return true;
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
        enum ResumeState
        {
            Parse = default,
            Bind,
            Describe,
            RowOrCompletion
        }

        ResumeState _state;
        public ReadOnlySequence<byte> Buffer { get; private set; }
        public MessageReader.ResumptionData ResumptionData { get; private set; }
        public RowDescription RowDescription { get; private set; }
        public bool CommandComplete { get; private set; }

        public ReadStatus Read(ref MessageReader reader)
        {
            switch (_state)
            {
                case ResumeState.Bind: goto bind;
                case ResumeState.Describe: goto describe;
                case ResumeState.RowOrCompletion: goto rowOrCompletion;
            }

            var status = new ParseComplete().Read(ref reader);
            if (status != ReadStatus.Done)
                return status;

            _state = ResumeState.Bind;
            bind:
            status = new BindComplete().Read(ref reader);
            if (status != ReadStatus.Done)
                return status;

            _state = ResumeState.Describe;
            describe:
            RowDescription = new RowDescription(pool: null);
            status = RowDescription.Read(ref reader); // TODO pool
            if (status != ReadStatus.Done)
                return status;

            _state = ResumeState.RowOrCompletion;
            rowOrCompletion:
            if (!reader.MoveNext())
                return ReadStatus.NeedMoreData;

            if (reader.Current.Code != BackendCode.DataRow && !reader.IsExpected(BackendCode.CommandComplete, out status, ensureBuffered: true))
                return status;

            if (reader.Current.Code == BackendCode.CommandComplete)
            {
                reader.ConsumeCurrent();
                CommandComplete = true;
                return ReadStatus.Done;
            }

            ResumptionData = reader.GetResumptionData();
            // Rewind the sequence to mark everything the row reader could want to use as unconsumed.
            reader.Reader.Rewind(reader.CurrentConsumed);
            Buffer = reader.Sequence.Slice(reader.Consumed);
            return ReadStatus.Done;
        }
    }

    readonly struct CompleteCommand : IBackendMessage
    {
        readonly long _consumed;

        public CompleteCommand(long consumed)
        {
            _consumed = consumed;
        }

        public ReadStatus Read(ref MessageReader reader)
        {
            reader.Reader.Advance(_consumed);
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
            if (IsMessageBuffered(_resumptionData.Header.Length - _resumptionData.MessageIndex))
            {
                _consumed += _resumptionData.Header.Length - _resumptionData.MessageIndex;
                _resumptionData = new MessageReader.ResumptionData(_resumptionData.Header, _resumptionData.Header.Length);
                if (_resumptionData.Header.Code == BackendCode.CommandComplete)
                {
                    status = ReadStatus.Done;
                    return false;
                }
            }
            else
            {
                status = ReadStatus.NeedMoreData;
                return false;
            }

            var buffer = _buffer.Slice(_consumed);
            if (!MessageHeader.TryParse(ref buffer, out var code, out var length))
            {
                status = ReadStatus.NeedMoreData;
                return false;
            }

            _consumed += MessageHeader.ByteCount;
            _resumptionData = new MessageReader.ResumptionData(new MessageHeader(code, length), MessageHeader.ByteCount);

            switch (code)
            {
                case BackendCode.DataRow:
                    // TODO Read column count and validate against row description columns in backend debug mode.
                    status = ReadStatus.Done;
                    return true;
                case BackendCode.CommandComplete:
                    // TODO Probably want to move this into the result set "invalid data" path and fix it there.
                    if (!IsMessageBuffered(length))
                    {
                        status = ReadStatus.NeedMoreData;
                        return false;
                    }

                    _consumed += length - MessageHeader.ByteCount;
                    _resumptionData = new MessageReader.ResumptionData(new MessageHeader(code, length), length);
                    status = ReadStatus.Done;
                    return false;
                default:
                    status = ReadStatus.InvalidData;
                    return false;
            }
        }

        bool IsMessageBuffered(uint remainingMessage)
            => remainingMessage <= _bufferLength - _consumed;
    }
}


class DataReader: IDisposable
{
    readonly PgV3Protocol _protocol;
    readonly CommandReader _commandReader;
    ReadActivation _readActivation;

    public DataReader(PgV3Protocol protocol)
    {
        _protocol = protocol;
        _commandReader = new CommandReader(protocol);
    }

    internal async ValueTask IntializeAsync(ReadActivation readActivation)
    {
        _readActivation = readActivation;
        // Immediately initialize the first result set, we're supposed to be positioned there at the start.
        await _commandReader.InitializeAsync().ConfigureAwait(false);
    }

    // TODO Could check completed here or in nextresultasync after awaiting and make sure Rfq is buffered, so we can do all that async, dispose would be guaranteed not to do IO.
    public Task<bool> ReadAsync(CancellationToken cancellationToken = default)
        => _commandReader.ReadAsync(cancellationToken);

    public void Dispose()
    {
        _readActivation?.Complete();
        // TODO Make sure _commandReader is completed (consume any remaining commands and rfq etc).
    }
} 

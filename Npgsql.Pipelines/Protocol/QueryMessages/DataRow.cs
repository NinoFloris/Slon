using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.QueryMessages;

class DataRow : IBackendMessage
{
    enum ReaderState
    {
        ReadColumns = 1,
        BufferColumns = 2,
        ConsumeRow = 3
    }

    ReaderState _state;
    bool _invalidData;

    public MessageReader.ResumptionData ResumptionData => _resumptionData;

    public int ColumnCount { get; private set; }
    public ReadOnlySequence<byte> Sequence;
    MessageReader.ResumptionData _resumptionData;

    public bool CommandComplete { get; private set; }
    public void SetInvalidData() => _invalidData = true;

    public void SetConsume(in MessageReader.ResumptionData resumptionData)
    {
        _resumptionData = resumptionData;
        _state = ReaderState.ConsumeRow;
    }

    public ReadStatus Read(ref MessageReader reader)
    {
        switch (_state)
        {
            case ReaderState.ReadColumns: goto ReadColumns;
            case ReaderState.BufferColumns: goto BufferColumns;
            case ReaderState.ConsumeRow: goto ConsumeRow;
        }

        if (!reader.MoveNext())
            return ReadStatus.NeedMoreData;

        if (reader.Current.Code != BackendCode.DataRow && !reader.IsExpected(BackendCode.CommandComplete, out var status, ensureBuffered: true))
            return status;

        if (reader.Current.Code == BackendCode.CommandComplete)
        {
            reader.ConsumeCurrent();
            CommandComplete = true;
            return ReadStatus.Done;
        }

        if (!reader.TryReadShort(out var columnCount))
            return ReadStatus.NeedMoreData;

        ColumnCount = columnCount;
        if (columnCount == 0 && !reader.ConsumeCurrent())
            return ReadStatus.NeedMoreData;
        _resumptionData = reader.GetResumptionData();

        ReadColumns:
        Sequence = reader.Sequence;
        _state = ReaderState.BufferColumns;
        return ReadStatus.Done;

        BufferColumns:
        if (_state == ReaderState.BufferColumns)
        {
            _state = ReaderState.ReadColumns;
            return ReadStatus.NeedMoreData;
        }

        ConsumeRow:
        reader = MessageReader.Resume(reader.Sequence, _resumptionData);

        if (!reader.ConsumeCurrent())
            return ReadStatus.NeedMoreData;

        return _invalidData ? ReadStatus.InvalidData : ReadStatus.Done;
    }

    public void Reset()
    {
        _state = 0;
        _resumptionData = default;
        _invalidData = false;
    }
}

class DataReader: IDisposable
{
    DataRow _currentRow = new();
    RowDescription _resultSetDescription;
    MessageReader.ResumptionData _resumptionData;
    readonly PgV3Protocol _protocol;

    public DataReader(PgV3Protocol protocol, RowDescription resultSetDescription)
    {
        _protocol = protocol;
        _resultSetDescription = resultSetDescription;
    }

    async ValueTask ExpandRow()
    {
        _currentRow = await _protocol.ReadMessageAsync(_currentRow);
    }

    async ValueTask ConsumeRow()
    {
        _currentRow.SetConsume(_resumptionData);
        _currentRow = await _protocol.ReadMessageAsync(_currentRow);
    }

    public async Task<bool> ReadAsync()
    {
        if (_currentRow.CommandComplete) return false;
        if (!_currentRow.ResumptionData.Header.IsDefault)
        {
            await ConsumeRow();
            _currentRow.Reset();
        }
        _currentRow = await _protocol.ReadMessageAsync(_currentRow);
        _resumptionData = _currentRow.ResumptionData;
        if (_currentRow.CommandComplete) return false;
        return true;
    }

    public async Task<T> GetFieldValueAsync<T>()
    {
        ReadStatus status;
        MessageReader.ResumptionData resumptionData = _resumptionData;
        T value = default!;
        do
        {
            status = ReadCore(_currentRow.Sequence, ref resumptionData, ref value);
            switch (status)
            {
                case ReadStatus.InvalidData:
                    _currentRow.SetInvalidData();
                    _resumptionData = resumptionData;
                    await ConsumeRow();
                    break;
                case ReadStatus.NeedMoreData:
                    await ExpandRow();
                    break;
            }
        } while (status != ReadStatus.Done);

        return value;

        static ReadStatus ReadCore<T>(in ReadOnlySequence<byte> sequence, ref MessageReader.ResumptionData resumptionData, ref T value)
        {
            var reader = MessageReader.Create(sequence, resumptionData, resumptionData.MessageIndex);
            if (!reader.TryReadInt(out var columnLength))
                return ReadStatus.NeedMoreData;

            if (typeof(T) == typeof(int))
                return IntReader(ref reader, out Unsafe.As<T, int>(ref value));

            if (typeof(T) == typeof(bool))
                return BoolReader(ref reader, out Unsafe.As<T, bool>(ref value));

            return ReadStatus.Done;
        }

        static ReadStatus BoolReader(ref MessageReader reader, out bool value) =>
            reader.TryReadBool(out value) ? ReadStatus.Done : ReadStatus.NeedMoreData;

        static ReadStatus IntReader(ref MessageReader reader, out int value) =>
            reader.TryReadInt(out value) ? ReadStatus.Done : ReadStatus.NeedMoreData;
    }

    public void Dispose()
    {
        _resultSetDescription.Dispose();
    }
} 

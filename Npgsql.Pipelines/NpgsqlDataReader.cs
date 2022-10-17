using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol;
using Npgsql.Pipelines.Protocol.PgV3;

namespace Npgsql.Pipelines;

enum ReaderState
{
    Uninitialized = 0,
    BeforeFirstMove,
    Active,
    Completed,
    Exhausted,
    Closed,
}

public sealed class NpgsqlDataReader: DbDataReader
{
    readonly Action<NpgsqlDataReader>? _returnAction;
    CommandReader _commandReader = null!; // Will be set during initialization.
    IOCompletionPair _ioCompletion;
    ReaderState _state;
    int _fieldCount;
    bool _hasRows;
    ulong? _recordsAffected;

    internal NpgsqlDataReader(Action<NpgsqlDataReader>? returnAction = null)
    {
        _returnAction = returnAction;
    }

    // This may throw io exceptions from either the write or read pipe.
    ValueTask<Operation> GetProtocol() => _ioCompletion.SelectAsync();

    internal async ValueTask<NpgsqlDataReader> IntializeAsync(IOCompletionPair ioCompletion, CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        _state = ReaderState.BeforeFirstMove;
        _ioCompletion = ioCompletion;
        var op = await GetProtocol().ConfigureAwait(false);
        _commandReader = op.Protocol.GetCommandReader();
        try
        {
            // Immediately initialize the first result set, we're supposed to be positioned there at the start.
            await NextResultAsyncInternal(op.Protocol, cancellationToken).ConfigureAwait(false);
        }
        catch(Exception exception)
        {
            // If this is a connection op this causes it to transition to broken, protocol ops will just ignore it.
            op.Complete(exception);
        }
        return this;
    }

    public override int Depth => 0;
    public override int FieldCount => _fieldCount;
    public override object this[int ordinal] => throw new NotImplementedException();
    public override object this[string name] => throw new NotImplementedException();
    public override int RecordsAffected
        => !_recordsAffected.HasValue
            ? -1
        : _recordsAffected > int.MaxValue
            ? throw new OverflowException(
            $"The number of records affected exceeds int.MaxValue. Use {nameof(Rows)}.")
            : (int)_recordsAffected;
    public ulong Rows => _recordsAffected ?? 0; 
    public override bool HasRows => _hasRows;
    public override bool IsClosed => _state > ReaderState.Closed || _state == ReaderState.Uninitialized;

    public override void Close()
    {
        throw new NotImplementedException();
    }

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        try
        {
            var hasNext = await _commandReader.ReadAsync(cancellationToken);
            if (!hasNext)
                SyncStates();
            return hasNext;
        }
        catch
        {
            SyncStates();
            throw;
        }
    }

    void SyncStates()
    {
        switch (_commandReader.State)
        {
            case CommandReaderState.Completed:
                HandleCompleted();
                break;
            case CommandReaderState.UnrecoverablyCompleted:
                HandleFailure();
                break;
        }

        void HandleCompleted()
        {
            // Store this before we move on.
            if (_state == ReaderState.Active)
            {
                _state = ReaderState.Completed;
                if (!_recordsAffected.HasValue)
                    _recordsAffected = 0;
                _recordsAffected += _commandReader.RowsAffected;
            }
        }

        void HandleFailure()
        {
            _state = ReaderState.Closed;
        }
    }

#if !NETSTANDARD2_0
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
    async ValueTask<bool> NextResultAsyncInternal(PgProtocol? protocol = null, CancellationToken cancellationToken = default)
    {
        // TODO walk the commands array and move to exhausted once done.
        try
        {
            protocol ??= (await GetProtocol().ConfigureAwait(false)).Protocol;
            await _commandReader.InitializeAsync(protocol, cancellationToken).ConfigureAwait(false);
            _state = ReaderState.Active;
            _fieldCount = _commandReader.FieldCount;
            _hasRows = _commandReader.HasRows;
            return true;
        }
        finally
        {
            SyncStates();
        }
    }

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        => NextResultAsyncInternal(cancellationToken: cancellationToken).AsTask();

    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public override bool GetBoolean(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override byte GetByte(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override DateTime GetDateTime(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override decimal GetDecimal(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override double GetDouble(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override Type GetFieldType(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override float GetFloat(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override Guid GetGuid(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override short GetInt16(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetInt32(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetInt64(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override string GetName(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetOrdinal(string name)
    {
        throw new NotImplementedException();
    }

    public override string GetString(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override object GetValue(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetValues(object[] values)
    {
        throw new NotImplementedException();
    }

    public override bool IsDBNull(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override bool NextResult()
    {
        throw new NotImplementedException();
    }

    public override bool Read()
    {
        throw new NotImplementedException();
    }


    public override IEnumerator GetEnumerator()
    {
        throw new NotImplementedException();
    }

#if !NETSTANDARD2_0
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
    async ValueTask DisposeCore(bool async)
    {
        if (_state is ReaderState.Uninitialized)
            return;

        _state = ReaderState.Uninitialized;
        var op = default(Operation);
        try
        {
            // Even await the task in the finally to make sure our return will always work, no matter the protocol state.
            op = await GetProtocol().ConfigureAwait(false);
            var state = _commandReader.State;
            if (state == CommandReaderState.Initialized)
            {
                try
                {
                    while (await ReadAsync())
                    {}
                    state = _commandReader.State;
                }
                catch (TimeoutException)
                {
                    state = CommandReaderState.Initialized;
                }
            }

            // TODO this might hang if writing somehow doesn't complete, maybe have a timeout and break the connection if its hit?
            if (state != CommandReaderState.UnrecoverablyCompleted && !_ioCompletion.Write.IsCompleted)
            {
                if (async)
                    await _ioCompletion.Write.ConfigureAwait(false);
                else
                    _ioCompletion.Write.GetAwaiter().GetResult();
            }

            // TODO update transaction status, also probably want this as a method on CommandReader instead.
            if (state == CommandReaderState.Completed)
            {
                var protocol = (PgV3Protocol)op.Protocol;
                try
                {
                    var rfq = async ? await protocol.ReadMessageAsync<ReadyForQuery>().ConfigureAwait(false) : protocol.ReadMessage<ReadyForQuery>();
                }
                catch(Exception ex)
                {
                    // If this is a connection op this causes it to transition to broken, protocol ops will just ignore it.
                    op.Complete(ex);
                }
            }
        }
        finally
        {
            _commandReader.Reset();
            _commandReader = null!;
            _ioCompletion = default;
            _returnAction?.Invoke(this);
            op.Dispose();
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            DisposeCore(false).GetAwaiter().GetResult();
    }

#if NETSTANDARD2_0
    public Task CloseAsync()
#else
    public override Task CloseAsync()
#endif
        => DisposeAsync().AsTask();

#if NETSTANDARD2_0
    public ValueTask DisposeAsync()
#else
    public override ValueTask DisposeAsync()
#endif
        => DisposeCore(true);
}

using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
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

// Implementation
public sealed partial class NpgsqlDataReader
{
    readonly Action<NpgsqlDataReader>? _returnAction;

    // Will be set during initialization.
    CommandReader? _commandReader;
    Command _command;

    ReaderState _state;
    int _fieldCount;
    bool _hasRows;
    ulong? _recordsAffected;

    Command GetCurrent() => _command;
    ValueTask<Operation> GetProtocol() => GetCurrent().GetProtocol();

    // Returns itself to limit the amount of statemachines required to be allocated to pipeline commands.
    // This is also why this async method is not pooled, it won't help as we'll quickly use the pooled instances (while paying for the lookups coming up empty every time).
    internal async ValueTask<NpgsqlDataReader> Intialize(bool async, Command command, CommandBehavior behavior, TimeSpan commandTimeout, CancellationToken cancellationToken = default)
    {
        _state = ReaderState.BeforeFirstMove;

        // This is an inlined version of NextResultAsyncCore to save on async overhead.
        CommandReader commandReader;
        try
        {
            commandReader = (await command.GetProtocol().ConfigureAwait(false)).Protocol.GetCommandReader();
            // _commandEnumerator = commands.GetEnumerator();
            // Immediately initialize the first command, we're supposed to be positioned there at the start.
            await commandReader.InitializeAsync(command, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // TODO maybe init should be a static method that has access to a pool?
            // We need to dispose right away as we won't even hand out the reader.
            await DisposeCore(true).ConfigureAwait(false);
            // Likely that Dispose throws errors but just in case that doesn't happen we'll rethrow our own.
            throw;
        }

        _command = command;
        _commandReader = commandReader;
        SyncStates();
        return this;
    }

    void SyncStates()
    {
        DebugShim.Assert(_commandReader is not null);
        switch (_commandReader.State)
        {
            case CommandReaderState.Completed:
                HandleCompleted();
                break;
            case CommandReaderState.UnrecoverablyCompleted:
                if (_state is not ReaderState.Uninitialized or ReaderState.Closed)
                    _state = ReaderState.Closed;
                break;
        }

        void HandleCompleted()
        {
            // Store this before we move on.
            if (_state is ReaderState.Active)
            {
                _state = ReaderState.Completed;
                if (!_recordsAffected.HasValue)
                    _recordsAffected = 0;
                _recordsAffected += _commandReader.RowsAffected;
            }
        }
    }

    [MemberNotNull(nameof(_commandReader))]
    void ThrowIfClosedOrDisposed(ReaderState? readerState = null)
    {
        switch (readerState ?? _state)
        {
            case ReaderState.Uninitialized:
                throw new ObjectDisposedException(nameof(NpgsqlDataReader));
            case ReaderState.Closed:
                throw new InvalidOperationException("Reader is closed.");
        }
        DebugShim.Assert(_commandReader is not null);
    }

    // Any updates should be reflected in Initialize.
    async Task<bool> NextResultAsyncCore(CancellationToken cancellationToken = default)
    {
        // TODO walk the commands array and move to exhausted once done.
        try
        {
            await _commandReader!.InitializeAsync(GetCurrent(), cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            SyncStates();
        }
    }

    async ValueTask CloseCore(bool async, ReaderState? readerState = null)
    {
        // TODO consume.
    }

#if !NETSTANDARD2_0
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
    async ValueTask DisposeCore(bool async)
    {
        // var readerState = InterlockedShim.Exchange(ref _state, ReaderState.Uninitialized);
        if (_state is ReaderState.Uninitialized)
            return;
        _state = ReaderState.Uninitialized;

        try
        {
            await CloseCore(async, _state).ConfigureAwait(false);
        }
        finally
        {
            var op = GetProtocol();
            // _commandReader?.Dispose();
            _commandReader = null;
            // _commandEnumerator.Dispose();
            // _commandEnumerator = default;
            _returnAction?.Invoke(this);
            // TODO
            op.Result.Complete();
        }
    }
}

// Public surface & ADO.NET
public sealed partial class NpgsqlDataReader: DbDataReader
{
    internal NpgsqlDataReader(Action<NpgsqlDataReader>? returnAction = null) => _returnAction = returnAction;

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
    public override bool IsClosed => _state is ReaderState.Closed or ReaderState.Uninitialized;

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        if (_state is ReaderState.Closed or ReaderState.Uninitialized)
            ThrowIfClosedOrDisposed();

        return await _commandReader!.ReadAsync(cancellationToken).ConfigureAwait(false);
    }

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        => NextResultAsyncCore(cancellationToken);

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

    public override void Close() => CloseCore(false).GetAwaiter().GetResult();

    protected override void Dispose(bool disposing)
        => DisposeCore(false).GetAwaiter().GetResult();

#if NETSTANDARD2_0
    public Task CloseAsync()
#else
    public override Task CloseAsync()
#endif
        => CloseCore(true).AsTask();

#if NETSTANDARD2_0
    public ValueTask DisposeAsync()
#else
    public override ValueTask DisposeAsync()
#endif
        => DisposeCore(true);
}

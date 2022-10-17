using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol;

namespace Npgsql.Pipelines;

public class NpgsqlConnection : DbConnection
{
    readonly NpgsqlDataSource _dataSource;
    OperationSlot? _operationSlot;
    ConnectionState _state;
    Exception? _breakException;

    OperationSlot GetSlot()
    {
        ThrowIfBroken();
        if (_operationSlot is null || _state is not (ConnectionState.Open or ConnectionState.Executing or ConnectionState.Fetching))
            throw new InvalidOperationException("Connection is not open or ready.");

        return _operationSlot;
    }

    internal NpgsqlConnection(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        throw new NotImplementedException();
    }

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotImplementedException();
    }

    [AllowNull]
    public override string ConnectionString { get; set; }
    public override ConnectionState State => _state;

    public override string Database => throw new NotImplementedException();
    public override string DataSource => throw new NotImplementedException();
    public override string ServerVersion => throw new NotImplementedException();

    protected override DbCommand CreateDbCommand() => new NpgsqlCommand(this);

    public override void Close()
    {
        MoveToClosed();

        // TODO Activate the pipeline with errors
        _operationSingleton = null;
        _operationSlot?.Task.Result.Complete();
    }

#if !NETSTANDARD2_0
    public override Task CloseAsync()
#else
    public Task CloseAsync()
#endif
    {
        Close();
        return Task.CompletedTask;
    }

    public override void Open()
    {
        MoveToConnecting();
        try
        {
            _operationSlot = _dataSource.Open(exclusiveUse: true, TimeSpan.FromSeconds(ConnectionTimeout));
            Debug.Assert(_operationSlot.Task.IsCompleted);
            _operationSlot.Task.GetAwaiter().GetResult();
            MoveToIdle();
        }
        catch
        {
            Close();
        }
    }

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        MoveToConnecting();
        try
        {
            // TODO add ConnectionTimeout, catch and throw 'data source exhausted'.
            // First we get a slot (could be a connection open but usually this is synchronous)
            var slot = await _dataSource.OpenAsync(exclusiveUse: true, TimeSpan.FromSeconds(ConnectionTimeout), cancellationToken).ConfigureAwait(false);
            _operationSlot = slot;
            // Then we await until the connection is fully ready for us (both tasks are covered by the same cancellationToken).
            // In non exclusive cases we already start writing our message as well but we choose not to do so here.
            // One of the reasons would be to be sure the connection is healthy once we transition to Open.
            // If we're still stuck in a pipeline we won't know for sure.
            await slot.Task;
            MoveToIdle();
        }
        catch
        {
            MoveToClosed();
            _state = ConnectionState.Closed;
            throw;
        }
    }

    void ThrowIfBroken()
    {
        if (_state is ConnectionState.Broken)
            throw new InvalidOperationException("Connection is in a broken state.", _breakException);
    }

    void MoveToClosed()
    {
        if (_state is ConnectionState.Broken)
            throw new InvalidOperationException("Connection is already broken.");

        _state = ConnectionState.Closed;
    }


    void MoveToConnecting()
    {
        if (_state is not (ConnectionState.Closed or ConnectionState.Broken))
            throw new InvalidOperationException("Connection is already open or being opened.");

        _state = ConnectionState.Connecting;
    }

    void MoveToExecuting()
    {
        Debug.Assert(_state is not (ConnectionState.Closed or ConnectionState.Broken), "Already Closed or Broken.");
        Debug.Assert(_state is ConnectionState.Open or ConnectionState.Executing or ConnectionState.Fetching, "Called on an unopened/not fetching/not executing connection.");
        // We allow pipelining so we can be fetching and executing, leave fetching in place.
        if (_state != ConnectionState.Fetching)
            _state = ConnectionState.Executing;
    }

    public void MoveToFetching()
    {
        Debug.Assert(_state is not (ConnectionState.Closed or ConnectionState.Broken), "Already Closed or Broken.");
        Debug.Assert(_state is ConnectionState.Open or ConnectionState.Executing or ConnectionState.Fetching, "Called on an unopened/not fetching/not executing connection.");
        _state = ConnectionState.Fetching;
    }

    public void MoveToBroken(Exception? exception = null)
    {
        // We'll just keep the first exception.
        if (_state is ConnectionState.Broken)
            return;

        Close();
        _breakException = exception;
        _state = ConnectionState.Broken;
    }

    public void MoveToIdle()
    {
        // No debug assert as completion can often happen in finally blocks, just check here.
        if (_state is not (ConnectionState.Closed or ConnectionState.Broken))
            _state = ConnectionState.Open;
    }

    internal readonly struct CommandWriter
    {
        readonly NpgsqlConnection _instance;

        public CommandWriter(NpgsqlConnection instance)
        {
            _instance = instance;
        }

        public IOCompletionPair WriteCommand(NpgsqlCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default)
        {
            var slot = _instance.GetSlot();
            // Assumption is that we can already begin reading, otherwise we'd need to tie the tasks together.
            Debug.Assert(slot.Task.IsCompletedSuccessfully);
            // First enqueue, only then call WriteCommandCore.
            var subSlot = EnqueueRead(slot);
            return new IOCompletionPair(WriteCommandCore(slot, command, behavior, cancellationToken), subSlot.Task);
        }

        async ValueTask WriteCommandCore(OperationSlot connectionSlot, NpgsqlCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default)
        {
            await BeginWrite(cancellationToken);
            try
            {
                _instance.MoveToExecuting();
                var pair = _instance._dataSource.WriteCommand(connectionSlot, command, behavior, cancellationToken);
                await pair.Write;
            }
            finally
            {
                EndWrite();
            }
        }

        ConnectionOperationSource EnqueueRead(OperationSlot connectionSlot)
        {
            lock (connectionSlot)
            {
                var source = _instance._pipelineTail = _instance.CreateSlotUnsynchronized(connectionSlot);
                // An immediately active read means head == tail, move to fetching immediately.
                if (source.Task.IsCompletedSuccessfully)
                    _instance.MoveToFetching();

                return source;
            }
        }

        Task BeginWrite(CancellationToken cancellationToken = default)
        {
            var writeLock = _instance._pipeliningWriteLock;
            if (writeLock is null)
            {
                var value = new SemaphoreSlim(1);
#pragma warning disable CS0197
                if (Interlocked.CompareExchange(ref _instance._pipeliningWriteLock, value, null) is null)
#pragma warning restore CS0197
                    writeLock = value;
            }

            if (!writeLock!.Wait(0))
                return writeLock.WaitAsync(cancellationToken);

            return Task.CompletedTask;
        }

        void EndWrite()
        {
            var writeLock = _instance._pipeliningWriteLock;
            if (writeLock?.CurrentCount == 0)
                writeLock.Release();
            else
                throw new InvalidOperationException("No write to end.");
        }
    }

    // Slots are thread safe up to the granularity of the slot, anything more is the responsibility of the caller.
    volatile SemaphoreSlim? _pipeliningWriteLock;
    volatile ConnectionOperationSource? _pipelineTail;
    ConnectionOperationSource? _operationSingleton;

    internal CommandWriter GetCommandWriter() => new(this);

    ConnectionOperationSource CreateSlotUnsynchronized(OperationSlot connectionSlot)
    {
        ConnectionOperationSource source;
        var current = _pipelineTail;
        if (current is null || current.IsCompleted)
        {
            var singleton = _operationSingleton;
            if (singleton is not null)
            {
                singleton.Reset();
                source = singleton;
            }
            else
                source = _operationSingleton = new ConnectionOperationSource(this, connectionSlot.Protocol, pooled: true);
        }
        else
        {
            source = new ConnectionOperationSource(this, connectionSlot.Protocol);
            current.SetNext(source);
        }

        return source;
    }

    void CompleteOperation(ConnectionOperationSource completed, ConnectionOperationSource? next, Exception? exception)
    {
        if (exception is not null)
            MoveToBroken(exception);
        else if (next is null)
            MoveToIdle();
        else
            next.Activate(_breakException);
    }

    class ConnectionOperationSource: OperationSource
    {
        readonly NpgsqlConnection _connection;
        ConnectionOperationSource? _next;

        public ConnectionOperationSource(NpgsqlConnection connection, PgProtocol? protocol, bool pooled = false) : base(protocol, pooled)
        {
            _connection = connection;
            // Pipelining on the same connection is expected to be done on the same thread.
            RunContinuationsAsynchronously = false;
        }

        public void Activate(Exception? ex = null) => ActivateCore(ex);

        public void SetNext(ConnectionOperationSource source)
        {
            if (Interlocked.CompareExchange(ref _next, source, null) == null)
                return;

            throw new InvalidOperationException("Next was already set.");
        }

        protected override void CompleteCore(PgProtocol protocol, Exception? exception)
            => _connection.CompleteOperation(this, _next, exception);

        protected override void ResetCore()
        {
            _next = null;
        }
    }
}

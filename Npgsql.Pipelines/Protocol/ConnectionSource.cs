using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.Protocol;

interface IConnectionFactory<T> where T : PgProtocol
{
    T Create(TimeSpan timeout);
    ValueTask<T> CreateAsync(CancellationToken cancellationToken);
}

class ConnectionSource<T>: IDisposable where T : PgProtocol
{
    readonly object?[] _connections;
    readonly IConnectionFactory<T> _factory;
    volatile bool _disposed;

    public ConnectionSource(IConnectionFactory<T> factory, int maxPoolSize)
    {
        if (maxPoolSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxPoolSize), "Cannot be zero or negative.");
        _connections = new object[maxPoolSize];
        _factory = factory;
    }

    object TakenSentinel => this;

    void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ConnectionSource<T>));
    }

    bool IsReadyConnection([NotNullWhen(true)] object? item, [MaybeNullWhen(false)]out T instance)
    {
        if (item is not null && !ReferenceEquals(TakenSentinel, item))
        {
            instance = Unsafe.As<object, T>(ref item);
            return instance.State == PgProtocolState.Ready;
        }

        instance = default;
        return false;
    }

    // Instead of ad-hoc we could also do this as an array sort on a timer as it only has to be an approximate best match.
    /// <summary>
    /// Either returns a non-null opSlot, or an index to create a new connection on.
    /// </summary>
    /// <param name="exclusiveUse"></param>
    /// <param name="allowPipelining"></param>
    /// <param name="slotIndex"></param>
    /// <param name="opSlot"></param>
    /// <param name="cancellationToken"></param>
    /// <returns>true if index or slot found, false if we couldn't pipeline onto any, otherwise always true.</returns>
    bool TryGetSlot(bool exclusiveUse, bool allowPipelining, out int slotIndex, out OperationSlot? opSlot, CancellationToken cancellationToken)
    {
        var connections = _connections;
        int candidateIndex;
        opSlot = null;
        do
        {
            (bool PendingExclusiveUse, int Pending) candidateKey = (true, Int32.MaxValue);
            candidateIndex = -1;
            T? candidateConn = null;
            OperationSlot? connOp;
            for (var i = 0; i < connections.Length; i++)
            {
                var item = connections[i];
                if (IsReadyConnection(item, out var conn))
                {
                    // If it's idle then that's the best scenario, return immediately.
                    if (conn.TryStartOperation(out connOp, OperationBehavior.ImmediateOnly | (exclusiveUse ? OperationBehavior.ExclusiveUse : OperationBehavior.None), cancellationToken))
                    {
                        slotIndex = i;
                        opSlot = connOp;
                        return true;
                    }

                    var currentKey = (conn.PendingExclusiveUse, conn.Pending);
                    if (!currentKey.PendingExclusiveUse && candidateKey.Pending > currentKey.Pending)
                    {
                        candidateKey = currentKey;
                        candidateIndex = i;
                        candidateConn = conn;
                    }
                }
                else if (Interlocked.CompareExchange(ref _connections[i], TakenSentinel, item) == item)
                {
                    // This is an empty/draining/completed slot which we can fill.
                    slotIndex = i;
                    opSlot = null;
                    return true;
                }
            }

            // Note: not 'ImmediateOnly' in the candidate flow as we're apparently exhausted.
            // TODO if we want full fairness we can put a channel in between all this
            // (an earlier caller can get stuck behind very slow ops, getting overtaken by 'luckier' callers).
            if (allowPipelining && candidateConn is not null &&
                candidateConn.TryStartOperation(out connOp, exclusiveUse ? OperationBehavior.ExclusiveUse : OperationBehavior.None, cancellationToken))
            {
                opSlot = connOp;
                slotIndex = candidateIndex;
            }
        } while (allowPipelining && opSlot is null);

        // Can only get here if we aren't allowed to pipeline or its all full.
        if (candidateIndex == -1)
        {
            slotIndex = default;
            return false;
        }

        slotIndex = candidateIndex;
        return true;
    }

    async ValueTask<OperationSlot> OpenConnection(int index, bool exclusiveUse, bool async, TimeSpan timeout = default, CancellationToken cancellationToken = default)
    {
        var item = Volatile.Read(ref _connections[index]);
        Debug.Assert(ReferenceEquals(item, TakenSentinel));
        T? conn;
        try
        {
            conn = await (async ? _factory.CreateAsync(cancellationToken) : new ValueTask<T>(_factory.Create(timeout)));
        }
        catch
        {
            // Remove the sentinel.
            Volatile.Write(ref _connections[index], null);
            throw;
        }

        if (_disposed)
        {
            conn.Dispose();
            ThrowIfDisposed();
        }

        if (!conn.TryStartOperation(out var connOp, OperationBehavior.ImmediateOnly | (exclusiveUse ? OperationBehavior.ExclusiveUse : OperationBehavior.None), CancellationToken.None))
            throw new InvalidOperationException("Could not start an operation on a fresh connection.");

        // Make sure this won't be reordered to make the instance visible to other threads before we get a spot.
        Volatile.Write(ref _connections[index], conn);
        return connOp;
    }

    public OperationSlot Get(bool exclusiveUse = false, TimeSpan timeout = default)
    {
        ThrowIfDisposed();
        if (!TryGetSlot(exclusiveUse, allowPipelining: false, out var index, out var opSlot, CancellationToken.None))
            throw new InvalidOperationException("ConnectionSource is exhausted, there are no empty slots or idle connections.");

        opSlot ??= OpenConnection(index, exclusiveUse, async: false, timeout).GetAwaiter().GetResult();
        Debug.Assert(opSlot is not null);
        Debug.Assert(opSlot!.Task.IsCompletedSuccessfully);
        return opSlot!;

    }

    public async ValueTask<OperationSlot> GetAsync(bool exclusiveUse = false, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (!TryGetSlot(exclusiveUse, allowPipelining: false, out var index, out var opSlot, CancellationToken.None))
            throw new InvalidOperationException("ConnectionSource is exhausted, there are no empty slots or connections idle enough to take new work.");

        opSlot ??= await OpenConnection(index, exclusiveUse, async: true, cancellationToken: cancellationToken);
        Debug.Assert(opSlot is not null);
        return opSlot!;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        foreach (var connection in _connections)
        {
            if (IsReadyConnection(connection, out var conn))
            {
                // We're just letting it run, draining can take a while and we're not going to wait.
                var _ = Task.Run(async () =>
                {
                    try
                    {
                        await conn.CompleteAsync();
                        conn.Dispose();
                    }
                    catch
                    {
                        // TODO This 'should' log something.
                    }
                });
            }
        }
    }
}

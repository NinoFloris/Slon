using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.Protocol;

abstract class OperationSlot
{
    /// <summary>
    /// The connection the slot belongs to, can be null once completed or when not yet bound.
    /// </summary>
    public abstract PgProtocol? Protocol { get; }

    public abstract bool IsCompleted { get; }

    /// <summary>
    /// The task will be activated once the operation can read from the connection.
    /// </summary>
    public abstract ValueTask<Operation> Task { get; }
}

abstract class OperationSource: OperationSlot
{
    [Flags]
    enum OperationSourceFlags
    {
        Created = 0,
        Pooled = 1,
        Activated = 2,
        Faulted = 4,
        Completed = 8,
        Canceled = 16
    }

    OperationSourceFlags _state;
    TaskCompletionSource<Operation> _tcs;
    CancellationTokenRegistration _cancellationRegistration;
    PgProtocol? _protocol;

    protected OperationSource(PgProtocol? protocol, bool asyncContinuations = true, bool pooled = false)
    {
        _tcs = new TaskCompletionSource<Operation>(asyncContinuations ? TaskCreationOptions.RunContinuationsAsynchronously : TaskCreationOptions.None);
        _protocol = protocol;
        if (pooled)
        {
            if (protocol is null)
                throw new ArgumentNullException(nameof(protocol), "Pooled sources cannot be unbound.");

            _tcs.SetResult(new Operation(this, protocol));
            _state = OperationSourceFlags.Pooled | OperationSourceFlags.Activated;
        }
        else
        {
            _state = OperationSourceFlags.Created;
        }
    }

    PgProtocol? TransitionToCompletion(CancellationToken token, Exception? exception = null)
    {
        var state = (OperationSourceFlags)Volatile.Read(ref Unsafe.As<OperationSourceFlags, int>(ref _state));
        // If we were already completed this is likely another completion from the activated code.
        if ((state & OperationSourceFlags.Completed) != 0)
            return null;

        var newState = (state & ~OperationSourceFlags.Activated) | OperationSourceFlags.Completed;
        if (token.IsCancellationRequested)
            newState |= OperationSourceFlags.Canceled;
        else if (exception is not null)
            newState |= OperationSourceFlags.Faulted;
        if (InterlockedShim.CompareExchange(ref _state, newState, state) == state)
        {
            // Only change the _tcs if we weren't already activated before.
            if ((state & OperationSourceFlags.Activated) == 0)
            {
                if (exception is not null)
                    _tcs.SetException(exception);
                else if (token.IsCancellationRequested)
                {
                    var result  = _tcs.TrySetCanceled(token);
                    DebugShim.Assert(result);
                }
            }
            return _protocol;
        }

        return null;
    }

    [MemberNotNullWhen(true, nameof(_protocol))]
    public bool IsPooled => (_state & OperationSourceFlags.Pooled) != 0;
    public bool IsActivated => (_state & OperationSourceFlags.Activated) != 0;
    [MemberNotNullWhen(true, nameof(_cancellationRegistration))]
    public bool IsCanceled => (_state & OperationSourceFlags.Canceled) != 0;
    public bool IsCompletedSuccessfully => IsCompleted && (_state & OperationSourceFlags.Faulted) != 0;

#if !NETSTANDARD2_0
    public CancellationToken CancellationToken => _cancellationRegistration.Token;
#else
    public CancellationToken CancellationToken {get; private set;}
#endif

    protected abstract void CompleteCore(PgProtocol protocol, Exception? exception);

    protected virtual void ResetCore() {}
    protected void BindCore(PgProtocol protocol)
    {
        if (Interlocked.CompareExchange(ref _protocol, protocol, null) != null)
            ThrowAlreadyBound();

        static void ThrowAlreadyBound() => throw new InvalidOperationException("Slot was already bound.");
    }

    protected void AddCancellation(CancellationToken cancellationToken)
    {
        if (IsPooled)
            throw new InvalidOperationException("Cannot cancel pooled sources.");

        if (!cancellationToken.CanBeCanceled)
            return;

        if (_cancellationRegistration != default)
            throw new InvalidOperationException("Cancellation already registered.");

#if NETSTANDARD2_0
        CancellationToken = cancellationToken;
#endif
        _cancellationRegistration = cancellationToken.UnsafeRegister((state, token) =>
        {
            ((OperationSource)state!).TransitionToCompletion(token);
        }, this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected void ActivateCore()
    {
        if (_protocol is null)
        {
            ThrowProtocolNull();
            return;
        }
        switch (InterlockedShim.CompareExchange(ref _state, OperationSourceFlags.Activated, OperationSourceFlags.Created))
        {
            case OperationSourceFlags.Activated:
                ThrowAlreadyActivated();
                break;
            case OperationSourceFlags.Created:
                _cancellationRegistration.Dispose();
                // Can be false when we were raced by cancellation completing the source right after we transitioned to the activated state.
                _tcs.TrySetResult(new Operation(this, _protocol));
                break;
        }

        [DoesNotReturn]
        [MemberNotNull(nameof(_protocol))]
        void ThrowProtocolNull() => throw new InvalidOperationException("Cannot activate an unbound source.");
        void ThrowAlreadyActivated() => throw new InvalidOperationException("Already activated.");
    }

    [MemberNotNullWhen(true, nameof(_protocol))]
    public sealed override bool IsCompleted => (_state & OperationSourceFlags.Completed) != 0;
    public sealed override PgProtocol? Protocol => IsCompleted ? null : _protocol;
    public sealed override ValueTask<Operation> Task => new(_tcs.Task);

    // Slot can already be completed due to cancellation.
    public bool TryComplete(Exception? exception = null)
    {
        PgProtocol? protocol;
        if ((protocol = TransitionToCompletion(CancellationToken.None, exception)) is not null)
        {
            CompleteCore(protocol, exception);
            return true;
        }

        return false;
    }

    public void Reset()
    {
        if (!IsPooled)
            throw new InvalidOperationException("Cannot reuse non-pooled sources.");
        _state = OperationSourceFlags.Pooled | OperationSourceFlags.Activated;
        ResetCore();
    }
}

readonly record struct Operation: IDisposable
{
    readonly OperationSource _source;

    internal Operation(OperationSource source, PgProtocol protocol)
    {
        _source = source;
        Protocol = protocol;
    }

    public PgProtocol Protocol { get; }

    public bool IsCompleted => _source.IsCompleted;
    // TODO if we couldn't complete with an exception we probably want to have the completing code kill the protocol instead.
    // Check how feasible it is that this (not being able to complete) would happen.
    public void Complete(Exception? exception = null) => _source?.TryComplete(exception);
    public void Dispose() => Complete();
}

readonly struct WriteResult
{
    const long UnknownBytesWritten = long.MinValue;
    public static WriteResult Unknown => new WriteResult(UnknownBytesWritten);

    public WriteResult(long bytesWritten)
    {
        BytesWritten = bytesWritten;
    }

    public long BytesWritten { get; }
}

readonly struct IOCompletionPair
{
    public IOCompletionPair(ValueTask<WriteResult> write, ValueTask<Operation> read)
    {
        Write = write.Preserve();
        Read = read.Preserve();
    }

    public ValueTask<WriteResult> Write { get; }
    public ValueTask<Operation> Read { get; }

    /// <summary>
    /// Checks whether Write or Read is completed (in that order) before waiting on either for one to complete until Read or both are.
    /// If Read is completed we don't wait for Write anymore but we will check its status on future invocations.
    /// </summary>
    /// <returns></returns>
    public ValueTask<Operation> SelectAsync()
    {
        // Return read when it is completed but only when write is completed successfully or still running.
        if (Write.IsCompletedSuccessfully || (!Write.IsCompleted && Read.IsCompleted))
            return Read;

        if (Write.IsFaulted || Write.IsCanceled)
        {
            Write.GetAwaiter().GetResult();
            return default;
        }

        // Neither are completed yet.
        return Core(this);

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        static async ValueTask<Operation> Core(IOCompletionPair instance)
        {
            await Task.WhenAny(instance.Write.AsTask(), instance.Read.AsTask()).ConfigureAwait(false);
            if (instance.Write.IsCompletedSuccessfully || (!instance.Write.IsCompleted && instance.Read.IsCompleted))
                return await instance.Read.ConfigureAwait(false);

            if (instance.Write.IsFaulted || instance.Write.IsCanceled)
            {
                instance.Write.GetAwaiter().GetResult();
                return default;
            }

            throw new InvalidOperationException("Should not get here");
        }
    }
}

enum PgProtocolState
{
    Created,
    Ready,
    Draining,
    Completed
}

[Flags]
enum OperationBehavior
{
    None = 0,
    ImmediateOnly = 1,
    ExclusiveUse = 2
}

static class OperationBehaviorExtensions
{
    public static bool HasImmediateOnly(this OperationBehavior behavior) => (behavior & OperationBehavior.ImmediateOnly) != 0;
    public static bool HasExclusiveUse(this OperationBehavior behavior) => (behavior & OperationBehavior.ExclusiveUse) != 0;
}

abstract class PgProtocol
{
    public abstract PgProtocolState State { get; }
    public abstract bool PendingExclusiveUse { get; }
    public abstract int Pending { get; }

    public abstract bool TryStartOperation([NotNullWhen(true)]out OperationSlot? slot, OperationBehavior behavior = OperationBehavior.None, CancellationToken cancellationToken = default);
    public abstract bool TryStartOperation(OperationSlot slot, OperationBehavior behavior = OperationBehavior.None, CancellationToken cancellationToken = default);
    // TODO deliberate whether cancellation makes sense.
    public abstract Task CompleteAsync(Exception? exception = null, CancellationToken cancellationToken = default);
    public abstract ValueTask FlushAsync(CancellationToken cancellationToken = default);
    public abstract ValueTask FlushAsync(OperationSlot op, CancellationToken cancellationToken = default);

    // TODO CommandReader is part of PgV3 atm.
    public abstract PgV3.CommandReader GetCommandReader();
}

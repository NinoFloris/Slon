using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Npgsql.Pipelines.Protocol;

abstract class OperationSlot
{
    /// <summary>
    /// The connection the slot belongs to, can be null once completed or when not yet bound.
    /// </summary>
    public abstract PgProtocol? Protocol { get; }

    [MemberNotNullWhen(false, nameof(Protocol))]
    public abstract bool IsCompleted { get; }

    /// <summary>
    /// The task that will be activated once the operation can read from the connection.
    /// </summary>
    public abstract ValueTask<Operation> Task { get; }
}

abstract class OperationSource: OperationSlot, IValueTaskSource<Operation>
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

    volatile OperationSourceFlags _state;
    ManualResetValueTaskSourceCore<Operation> _tcs; // mutable struct; do not make this readonly
    CancellationTokenRegistration? _cancellationRegistration;
    PgProtocol? _protocol;

    [MemberNotNullWhen(true, nameof(_protocol))]
    bool IsPooled => (_state & OperationSourceFlags.Pooled) != 0;

    protected OperationSource(PgProtocol? protocol, bool pooled = false)
    {
        _tcs = new ManualResetValueTaskSourceCore<Operation> { RunContinuationsAsynchronously = true };
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

    public override bool IsCompleted => (_state & OperationSourceFlags.Completed) != 0;
    public override PgProtocol? Protocol => IsCompleted ? null : _protocol;
    public override ValueTask<Operation> Task => new(this, _tcs.Version);

    protected bool RunContinuationsAsynchronously
    {
        get => _tcs.RunContinuationsAsynchronously;
        set => _tcs.RunContinuationsAsynchronously = value;
    }

    protected void BindCore(PgProtocol protocol)
    {
        if (Interlocked.CompareExchange(ref _protocol, protocol, null) != null)
            throw new InvalidOperationException("Already bound.");
    }

    PgProtocol? TransitionToCompletion(bool cancellation)
    {
        var state = _state;
        var newState = (state & ~OperationSourceFlags.Activated) | OperationSourceFlags.Completed;
        if (cancellation)
            newState |= OperationSourceFlags.Canceled;
        if (Interlocked.CompareExchange(ref Unsafe.As<OperationSourceFlags, int>(ref _state), (int)newState, (int)state) == (int)state)
        {
            _tcs.Reset();
            return _protocol;
        }

        return null;
    }
    protected void AddCancellation(CancellationToken cancellationToken)
    {
        if (IsPooled)
            throw new InvalidOperationException("Cannot cancel pooled sources.");

        if (!cancellationToken.CanBeCanceled)
            return;

        if (_cancellationRegistration is not null)
            throw new InvalidOperationException("Cancellation already added");

        _cancellationRegistration = cancellationToken.Register(state =>
        {
            ((OperationSource)state!).TransitionToCompletion(cancellation: true);
        }, this);
    }

    protected void ActivateCore(Exception? exception)
    {
        if (IsPooled)
            throw new InvalidOperationException("Cannot activate pooled source.");

        if (_protocol is null)
            throw new InvalidOperationException("Cannot activate an unbound source.");

        // The only thing we cannot check for is Completed as that may race with cancellation.
        if ((_state & OperationSourceFlags.Activated) != 0)
            throw new InvalidOperationException("Already activated or completed");

        var newState = OperationSourceFlags.Activated;
        if (exception is not null)
            newState |= OperationSourceFlags.Faulted;

        if (Interlocked.CompareExchange(ref Unsafe.As<OperationSourceFlags, int>(ref _state), (int)newState, (int)OperationSourceFlags.Created) == (int)OperationSourceFlags.Created)
        {
            if (exception is not null)
                _tcs.SetException(exception);

            _tcs.SetResult(new Operation(this, _protocol!));
        }
    }

    protected abstract void CompleteCore(PgProtocol protocol, Exception? exception);

    public void Complete(Exception? exception = null)
    {
        PgProtocol? protocol;
        if ((protocol = TransitionToCompletion(cancellation: false)) is not null)
        {
            _cancellationRegistration?.Dispose();
            CompleteCore(protocol, exception);
        }
    }

    protected abstract void ResetCore();

    public void Reset()
    {
        if (!IsPooled)
            throw new InvalidOperationException("Cannot reuse non-pooled sources.");

        if (IsCompleted)
            _tcs.SetResult(new Operation(this, _protocol));
        _state = OperationSourceFlags.Pooled | OperationSourceFlags.Activated;
        ResetCore();
    }

    Operation IValueTaskSource<Operation>.GetResult(short token) => _tcs.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<Operation>.GetStatus(short token) => _tcs.GetStatus(token);
    void IValueTaskSource<Operation>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _tcs.OnCompleted(continuation, state, token, flags);
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
    public void Complete(Exception? exception = null) => _source?.Complete();
    public void Dispose() => Complete();
}

readonly struct IOCompletionPair
{
    public IOCompletionPair(ValueTask write, ValueTask<Operation> read)
    {
        Write = write.Preserve();
        Read = read.Preserve();
    }

    public ValueTask Write { get; }
    public ValueTask<Operation> Read { get; }

    /// <summary>
    /// Checks whether Write or Read is completed (in that order) before waiting on either for one to complete until Read or both are.
    /// If Read is completed we don't wait for Write anymore but we will check its status on future invocations.
    /// </summary>
    /// <returns></returns>
    public ValueTask<Operation> SelectAsync()
    {
        if (Write.IsCompletedSuccessfully || (!Write.IsCompleted && Read.IsCompleted))
            return Read;

        if (Write.IsFaulted || Write.IsCanceled)
        {
            Write.GetAwaiter().GetResult();
            return default;
        }

        return Core(this);

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        static async ValueTask<Operation> Core(IOCompletionPair instance)
        {
            await Task.WhenAny(instance.Write.AsTask(), instance.Read.AsTask());
            if (instance.Write.IsCompletedSuccessfully || (!instance.Write.IsCompleted && instance.Read.IsCompleted))
                return await instance.Read;

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

abstract class PgProtocol: IDisposable
{
    public abstract PgProtocolState State { get; }
    public abstract bool PendingExclusiveUse { get; }
    public abstract int Pending { get; }

    public abstract bool TryStartOperation([NotNullWhen(true)]out OperationSlot? operationSlot, OperationBehavior behavior = OperationBehavior.None, CancellationToken cancellationToken = default);
    public abstract Task CompleteAsync(CancellationToken cancellationToken = default);

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}

using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Slon.Protocol.PgV3;

namespace Slon.Protocol;

enum ProtocolState
{
    Created,
    Ready,
    Draining,
    Completed
}

abstract class Protocol
{
    public abstract ProtocolState State { get; }
    public abstract bool PendingExclusiveUse { get; }
    public abstract int Pending { get; }

    public abstract bool TryStartOperation([NotNullWhen(true)]out OperationSlot? slot, OperationBehavior behavior = OperationBehavior.None, CancellationToken cancellationToken = default);
    public abstract bool TryStartOperation(OperationSlot slot, OperationBehavior behavior = OperationBehavior.None, CancellationToken cancellationToken = default);
    public abstract Task CompleteAsync(Exception? exception = null);
    public abstract ValueTask FlushAsync(CancellationToken cancellationToken = default);
    public abstract ValueTask FlushAsync(OperationSlot op, CancellationToken cancellationToken = default);

    // TODO maybe this doesn't belong here, we'd need some other place that pools these though.
    // TODO this is part of PgV3 though there is a fairly abstract api ready inside of it.
    public abstract PgV3CommandReader GetCommandReader();
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

using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.Protocol;

enum PgProtocolState
{
    Created,
    Ready,
    Draining,
    Completed
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

using System.Threading;

namespace Npgsql.Pipelines.Protocol;

abstract class CommandWriter<TValues, TExecution>
{
    public abstract CommandContext<TExecution> WriteAsync<TCommand>(OperationSlot slot, ref TCommand command, bool flushHint = true, CancellationToken cancellationToken = default) where TCommand : ICommand<TValues, TExecution>;
}
using System.Threading;

namespace Slon.Protocol;

abstract class CommandWriter<TValues, TExecution>
{
    public abstract CommandContext<TExecution> WriteAsync<TCommand>(OperationSlot slot, in TCommand command, bool flushHint = true, CancellationToken cancellationToken = default) where TCommand : ICommand<TValues, TExecution>;
}

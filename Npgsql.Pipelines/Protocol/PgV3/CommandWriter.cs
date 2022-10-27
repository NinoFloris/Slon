using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol.PgV3.Commands;
using Npgsql.Pipelines.Protocol.PgV3.Types;

namespace Npgsql.Pipelines.Protocol.PgV3;

class CommandWriter
{
    public static CommandContext WriteExtendedAsync<TCommand>(OperationSlot slot, TCommand command, bool flushHint = true, CancellationToken cancellationToken = default)
        where TCommand : ICommand
    {
        if (slot.Protocol is not PgV3Protocol protocol)
            throw new ArgumentException($"Cannot write with a slot for a different protocol type, expected: {nameof(PgV3Protocol)}.", nameof(slot));

        var values = command.GetValues();

        // If we have a statement *and* our connection still has to prepare, do so.
        ICommandSession? session = null;
        string? statementName = null;
        if (values.Statement is not null && protocol.GetOrAddStatementName(values.Statement, out statementName))
            session = command.StartSession(values);

        return CommandContext.Create(
            protocol.WriteMessageBatchAsync(slot, Core, (values, statementName), flushHint, cancellationToken),
            values.ExecutionFlags,
            session
        );

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
        static async ValueTask Core(PgV3Protocol.BatchWriter writer, (ICommand.Values, string?) state, CancellationToken cancellationToken)
        {
            var (values, statementName) = state;
            var executionFlags = values.ExecutionFlags;
            var portal = string.Empty;

            if (executionFlags.HasPreparing())
                await writer.WriteMessageAsync(new Parse(values.StatementText, values.Parameters, statementName), cancellationToken).ConfigureAwait(false);

            await writer.WriteMessageAsync(new Bind(portal, values.Parameters, ResultColumnCodes.CreateOverall(FormatCode.Binary), statementName), cancellationToken).ConfigureAwait(false);

            if (executionFlags.HasPreparing())
                await writer.WriteMessageAsync(Describe.CreateForPortal(portal), cancellationToken).ConfigureAwait(false);

            await writer.WriteMessageAsync(new Execute(portal), cancellationToken).ConfigureAwait(false);

            if (executionFlags.HasErrorBarrier())
                await writer.WriteMessageAsync(new Sync(), cancellationToken).ConfigureAwait(false);
        }
    }
}

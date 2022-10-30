using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.Protocol.PgV3;

class CommandWriter
{
    // Close input sessions after writing is done, free to re-use or change the underlying instance after that.
    static void CloseInputParameterSessions(ReadOnlyMemory<KeyValuePair<CommandParameter, IParameterWriter>> parameters)
    {
        // TODO optimize, probably want an actual type for Parameters, to state some useful facts gathered during building (has named, has sessions etc).
        foreach (var (p, _) in parameters.Span)
        {
            if (p.TryGetParameterSession(out var session) && session.Kind is ParameterKind.Input)
                session.Close();
        }
    }

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
            try
            {
                if (!executionFlags.HasPrepared())
                    await writer.WriteMessageAsync(new Parse(values.StatementText, values.Parameters, statementName), cancellationToken).ConfigureAwait(false);

                await writer.WriteMessageAsync(new Bind(portal, values.Parameters, ResultColumnCodes.CreateOverall(Types.FormatCode.Binary), statementName), cancellationToken).ConfigureAwait(false);

                if (!executionFlags.HasPrepared())
                    await writer.WriteMessageAsync(Describe.CreateForPortal(portal), cancellationToken).ConfigureAwait(false);

                await writer.WriteMessageAsync(new Execute(portal), cancellationToken).ConfigureAwait(false);

                if (executionFlags.HasErrorBarrier())
                    await writer.WriteMessageAsync(new Sync(), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (!values.Parameters.IsEmpty)
                    CloseInputParameterSessions(values.Parameters);
            }
        }
    }
}

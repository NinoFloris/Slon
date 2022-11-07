using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace Npgsql.Pipelines.Protocol.PgV3;

class CommandWriter
{
    // Note: ref command to allow structs to mutate themselves in StartExecution.
    public static CommandContext WriteExtendedAsync<TCommand>(OperationSlot slot, ref TCommand command, CommandParameters parameters, ExecutionFlags additionalFlags, bool flushHint = true, CancellationToken cancellationToken = default) where TCommand: ICommand
    {
        // We need to start the command execution before writing to prevent any races, as the read slot could already be completed.
        var values = command.GetValues(parameters, additionalFlags);
        var commandExecution = command.CreateExecution(values with { ExecutionFlags = GetEffectiveExecutionFlags(slot, values, out var statementName) });
        var completionPair = ((PgV3Protocol)slot.Protocol!).WriteMessageAsync(slot, new Command(values, statementName), flushHint, cancellationToken);
        return CommandContext.Create(completionPair, commandExecution);

        // Resolves the execution flags in the context of a specific protocol instance with regard to preparation.
        static ExecutionFlags GetEffectiveExecutionFlags(OperationSlot slot, in ICommand.Values values, out string? statementName)
        {
            if (slot.Protocol is not PgV3Protocol protocol)
            {
                ThrowInvalidSlot();
                statementName = null;
                return default;
            }

            if (values.Statement is not null)
            {
                // If we have a statement *and* our connection still has to prepare, do so.
                if (protocol.GetOrAddStatementName(values.Statement, out statementName))
                    return (values.ExecutionFlags & ~ExecutionFlags.Unprepared) | ExecutionFlags.Preparing;

                // If our connection has it prepared we run it directly with the statementName
                return (values.ExecutionFlags & ~ExecutionFlags.Unprepared) | ExecutionFlags.Prepared;
            }

            statementName = null;
            return values.ExecutionFlags;
        }
    }

    [DoesNotReturn]
    static void ThrowInvalidSlot()
        => throw new ArgumentException($"Cannot use a slot for a different protocol type, expected: {nameof(PgV3Protocol)}.", "slot");

    readonly struct Command: IFrontendMessage
    {
        readonly ICommand.Values _values;
        readonly string? _statementName;

        public Command(ICommand.Values values, string? statementName)
        {
            _values = values;
            _statementName = statementName;
        }

        // TODO bring back async writing for large binds (needs a sum and a treshold of precomputed parameter lengths).
        public bool CanWrite => true;
        public void Write<T>(ref SpanBufferWriter<T> buffer) where T : IBufferWriter<byte>
        {
            try
            {
                var portal = string.Empty;
                if (!_values.ExecutionFlags.HasPrepared())
                    Parse.WriteMessage(ref buffer, _values.StatementText, _values.CommandParameters.Collection, _statementName);

                // Bind is rather big, duplicating the static writing and IFrontendMessage paths becomes rather bloaty, just new the struct.
                new Bind(portal, _values.CommandParameters.Collection, ResultColumnCodes.CreateOverall(Types.FormatCode.Binary), _statementName).Write(ref buffer);

                if (!_values.ExecutionFlags.HasPrepared())
                    Describe.WriteForPortal(ref buffer, portal);

                Execute.WriteMessage(ref buffer, portal);

                if (_values.ExecutionFlags.HasErrorBarrier())
                    Sync.WriteMessage(ref buffer);
            }
            finally
            {
                if (!_values.CommandParameters.Collection.IsEmpty)
                    CloseInputParameterSessions(_values.CommandParameters.Collection);
            }
        }

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
    }
}

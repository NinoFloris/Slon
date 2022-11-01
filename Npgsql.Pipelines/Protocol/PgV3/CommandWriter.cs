using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CommandContext WriteExtendedAsync(OperationSlot slot, ICommand command, bool flushHint = true, CancellationToken cancellationToken = default)
    {
        if (slot.Protocol is not PgV3Protocol protocol)
            ThrowInvalidSlot();

        var values = command.GetValues();

        // If we have a statement *and* our connection still has to prepare, do so.
        ICommandSession? session = null;
        string? statementName = null;
        if (values.Statement is not null && protocol.GetOrAddStatementName(values.Statement, out statementName))
            session = command.StartSession(values);

        return CommandContext.Create(
            protocol.WriteMessageAsync(slot, new Command(values, statementName), flushHint, cancellationToken),
            values.ExecutionFlags,
            session
        );

        static void ThrowInvalidSlot()
            => throw new ArgumentException($"Cannot write with a slot for a different protocol type, expected: {nameof(PgV3Protocol)}.", nameof(slot));
    }

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
        public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
        {
            try
            {
                var portal = string.Empty;
                if (!_values.ExecutionFlags.HasPrepared())
                    Parse.WriteMessage(ref buffer, _values.StatementText, _values.Parameters, _statementName);

                // Bind is rather big, duplicating the static writing and IFrontendMessage paths becomes rather bloaty, just new the struct.
                new Bind(portal, _values.Parameters, ResultColumnCodes.CreateOverall(Types.FormatCode.Binary), _statementName).Write(ref buffer);

                if (!_values.ExecutionFlags.HasPrepared())
                    Describe.WriteForPortal(ref buffer, portal);

                Execute.WriteMessage(ref buffer, portal);

                if (_values.ExecutionFlags.HasErrorBarrier())
                    Sync.WriteMessage(ref buffer);
            }
            finally
            {
                if (!_values.Parameters.IsEmpty)
                    CloseInputParameterSessions(_values.Parameters);
            }
        }
    }
}

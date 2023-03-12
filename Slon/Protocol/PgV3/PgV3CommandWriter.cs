using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Slon.Buffers;
using Slon.Pg;
using Slon.Protocol.Pg;

namespace Slon.Protocol.PgV3;

sealed class PgV3CommandWriter: CommandWriter<CommandValues, CommandExecution>
{
    readonly PgTypeCatalog _typeCatalog;
    readonly Encoding _encoding;
    readonly Func<PgV3Statement, PgV3Statement> _agnosticStatementMapper;

    public PgV3CommandWriter(PgTypeCatalog typeCatalog, Encoding encoding, Func<PgV3Statement, PgV3Statement> agnosticStatementMapper)
    {
        _typeCatalog = typeCatalog;
        _encoding = encoding;
        _agnosticStatementMapper = agnosticStatementMapper;
    }

    public override CommandContext<CommandExecution> WriteAsync<TCommand>(OperationSlot slot, in TCommand command, bool flushHint = true, CancellationToken cancellationToken = default)
    {
        if (slot.Protocol is not PgV3Protocol protocol)
        {
            ThrowInvalidSlot();
            return default;
        }

        if (command is not IPgCommand pgCommand)
        {
            ThrowInvalidCommand();
            return default;

        }

        var values = pgCommand.GetValues();
        CommandContext<CommandExecution> result;
        try
        {
            var statement = ResolveEffectiveStatement(values.Statement);
            values = values with
            {
                StatementText = values.StatementText.WithEncoding(_encoding),
                Statement = statement,
                ExecutionFlags = GetEffectiveExecutionFlags(protocol, statement, values.ExecutionFlags, out var statementName)
            };
            // We need to create the command execution before writing to prevent any races, as the read slot could already be completed.
            var commandExecution = pgCommand.BeginExecution(values);
            // We don't actually await the writes here (and no sync exceptions happen).
            // This also assures we don't cleanup sessions twice on an exception (once inside the command and once here).
            var completionPair = protocol.WriteMessageAsync(slot, new Command(values, statementName, protocol, _encoding, _typeCatalog), flushHint, cancellationToken);
            result = CommandContext<CommandExecution>.Create(completionPair, commandExecution);
        }
        catch
        {
            CleanupParameters(values.Additional.ParameterContext, inputOnlyUntilException: false);
            throw;
        }
        return result;
    }

    // Resolves any agnostic statements to their backend specific kind.
    PgV3Statement? ResolveEffectiveStatement(Statement? statement)
    {
        if (statement is null)
            return null;

        if (statement is not PgV3Statement v3Statement)
        {
            ThrowInvalidStatement();
            return null;
        }

        if (v3Statement.IsBackendAgnosticStatement)
        {
            var result = _agnosticStatementMapper(v3Statement);
            DebugShim.Assert(!result.IsBackendAgnosticStatement);
            return result;
        }

        return v3Statement;
    }

    // Resolves the execution flags in the context of a specific protocol instance with regard to preparation.
    static ExecutionFlags GetEffectiveExecutionFlags(PgV3Protocol protocol, Statement? statement, ExecutionFlags flags, out SizedString? statementName)
    {
        if (statement is not null)
        {
            // If we have a statement *and* our connection still has to prepare, do so.
            var result = protocol.GetOrAddStatementName(statement, out var name);
            statementName = name;
            if (result)
                return (flags & ~(ExecutionFlags.Unprepared | ExecutionFlags.Prepared)) | ExecutionFlags.Preparing;

            // If our connection has it prepared we run it directly with the statementName
            return (flags & ~(ExecutionFlags.Unprepared | ExecutionFlags.Preparing)) | ExecutionFlags.Prepared;
        }

        statementName = null;
        return flags;
    }

    // This estimate is used for upper bound parameter size calculations.
    // We may never return a value larger than bufferSegmentSize, and to prevent partial flushes during Bind
    // (to get an empty buffer segment) we should try to be conservative.
    public static int EstimateParameterBufferSize(int bufferSegmentSize, string? statementText = null)
    {
        return bufferSegmentSize / 2;
    }

    static void ThrowInvalidSlot()
        => throw new ArgumentException($"Cannot use a slot for a different protocol type, expected: {nameof(PgV3Protocol)}.", "slot");

    static void ThrowInvalidCommand()
        => throw new ArgumentException($"Cannot use a command for a different protocol type, expected: {nameof(IPgCommand)}.", "command");

    static void ThrowInvalidStatement()
        => throw new ArgumentException($"Cannot handle Statement for a different protocol type, expected: {nameof(PgV3Statement)}.", "statement");

    readonly struct Command: IFrontendMessage
    {
        readonly PgV3Protocol _protocol;
        readonly Encoding _encoding;
        readonly PgTypeCatalog _typeCatalog;
        readonly IPgCommand.Values _values;
        readonly SizedString? _statementName;

        public Command(IPgCommand.Values values, SizedString? statementName, PgV3Protocol protocol, Encoding encoding, PgTypeCatalog typeCatalog)
        {
            _values = values;
            _statementName = statementName;
            _protocol = protocol;
            _encoding = encoding;
            _typeCatalog = typeCatalog;
        }

        // TODO bring back streaming writing for large binds (needs a sum and a threshold of precomputed parameter lengths).
        public bool CanWrite => true;
        public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
        {
            var portal = string.Empty;
            var encoding = _encoding;
            var parameterContext = _values.Additional.ParameterContext;
            try
            {
                if (!_values.ExecutionFlags.HasPrepared())
                    Parse.WriteMessage(ref buffer, _typeCatalog, _values.StatementText, parameterContext.Parameters, _statementName, encoding);

                // Bind is rather big, duplicating things for a static WriteMessage/WriteStreamingMessage becomes rather bloaty, just new the struct.
                using var parametersWriter = new ParametersWriter(parameterContext, _protocol, _typeCatalog, FlushMode.None); // Don't do anything during flushing, it's all buffered.
                new Bind(portal, parametersWriter, _values.Additional.RowRepresentation, _statementName, encoding).Write(ref buffer);

                // We don't have to describe if we already known the statement fields from another connection preparation.
                if (!_values.ExecutionFlags.HasPrepared() || !_values.Statement!.IsComplete)
                    Describe.WriteForPortal(ref buffer, portal, encoding);

                Execute.WriteMessage(ref buffer, portal, encoding);

                if (_values.Additional.Flags.HasErrorBarrier())
                    Sync.WriteMessage(ref buffer);
            }
            catch
            {
                CleanupParameters(parameterContext, inputOnlyUntilException: false);
                throw;
            }
            CleanupParameters(parameterContext);
        }
    }

    // Used to cleanup input parameters (and writestate of all) immediately after writing is done, this allows for faster pooled object reuse.
    // Note this method should be changed with care, it needs to make sure it does not do any double closes of sessions.
    // Multiple sessions may be backed by the same instance and reused, therefore making sure its internal state stays correct is important.
    static void CleanupParameters(ParameterContext context, bool inputOnlyUntilException = true)
    {
        var anySessions = context.HasSessions();
        if (!anySessions && !context.HasWriteState())
            return;

        var exceptionIndex = -1;
        Exception? exception = null;
        var parameters = context.Parameters.Span;
        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];
            // Wrap it as IParameterSession.Close is unknown code.
            try
            {
                if (anySessions && parameter.TryGetParameterSession(out var session))
                {
                    if ((inputOnlyUntilException && session.Kind is ParameterKind.Input) || !inputOnlyUntilException)
                        session.Close();
                }
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            // We can always dispose any write state, it's never needed past this point.
            // We wrap it as ConverterInfo.DisposeWriteState might at some point call into converters.
            try
            {
                if (parameter.WriteState is not null)
                    parameter.Writer.Info.DisposeWriteState(parameter.WriteState);
            }
            catch (Exception ex)
            {
                exception ??= ex;
            }

            if (exception is not null)
            {
                exceptionIndex = i;
                break;
            }
        }

        if (exceptionIndex != -1)
            CleanupUncommon(parameters);

        // We can also dispose the context itself sooner if there are no output sessions.
        if (!anySessions || !context.HasWritableParamSessions())
            context.Dispose();

        void CleanupUncommon(ReadOnlySpan<Parameter> parameters)
        {
            // Once we have an exception we start cleaning up everything else.
            List<Exception>? exceptions = null;
            if (exception is not null)
            {
                // Anything before the exception only needs non input kind sessions cleaned up.
                for (var i = 0; i < exceptionIndex && i < parameters.Length; i++)
                {
                    var parameter = parameters[i];
                    // Wrap it as IParameterSession.Close is unknown code.
                    try
                    {
                        if (anySessions && parameter.TryGetParameterSession(out var session) && session.Kind != ParameterKind.Input)
                            session.Close();
                    }
                    catch (Exception ex)
                    {
                        (exceptions ??= new() { exception }).Add(ex);
                    }
                }

                // Anything after the exception should just cleanup everything.
                for (var i = exceptionIndex + 1; i < parameters.Length; i++)
                {
                    var parameter = parameters[i];
                    // Wrap it as IParameterSession.Close is unknown code.
                    try
                    {
                        if (anySessions && parameter.TryGetParameterSession(out var session))
                            session.Close();
                    }
                    catch (Exception ex)
                    {
                        (exceptions ??= new() { exception }).Add(ex);
                    }

                    // We wrap it as ConverterInfo.DisposeWriteState might at some point call into converters.
                    try
                    {
                        if (parameter.WriteState is not null)
                            parameter.Writer.Info.DisposeWriteState(parameter.WriteState);
                    }
                    catch (Exception ex)
                    {
                        (exceptions ??= new() { exception }).Add(ex);
                    }
                }

                context.Dispose();
                throw exceptions is null ? exception : new AggregateException(exceptions);
            }
        }
    }
}

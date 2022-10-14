using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.Protocol;

public enum CommandKind
{
    Unprepared,
    Prepared
}

interface ICommandInfo
{
    public CommandKind CommandKind { get; }
    public ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> Parameters { get; }
    public bool AppendErrorBarrier { get; }
    public string? CommandText { get; }
    public string? PreparedStatementName { get; }
}

static class CommandInfoExtensions
{
    public static void Validate(this ICommandInfo command)
    {
        switch (command.CommandKind)
        {
            case CommandKind.Prepared:
                if (command.PreparedStatementName is null or "")
                    throw new ArgumentException("PreparedStatementName cannot be null or empty for a prepared command.");
                break;
            case CommandKind.Unprepared:
                if (command.CommandText is null)
                    throw new ArgumentException("CommandText cannot be null for an unprepared command.");
                break;
        }
    }
}

class CommandWriter
{
    readonly PgV3Protocol _protocol;
    public CommandWriter(PgV3Protocol protocol)
    {
        _protocol = protocol;
    }

    public IOCompletionPair WriteExtendedAsync(ICommandInfo commandInfo, bool flushHint = true, CancellationToken cancellationToken = default)
        => WriteExtendedAsync(_protocol, commandInfo, flushHint, cancellationToken);

    public static IOCompletionPair WriteExtendedAsync(PgV3Protocol protocol, ICommandInfo commandInfo, bool flushHint = true, CancellationToken cancellationToken = default)
    {
        commandInfo.Validate();
        return protocol.WriteMessageBatchAsync(static async (writer, commandInfo, cancellationToken) =>
        {
            var portal = string.Empty;
            switch (commandInfo.CommandKind)
            {
                case CommandKind.Prepared:
                    await writer.WriteMessageAsync(new Bind(portal, commandInfo.Parameters, ResultColumnCodes.CreateOverall(FormatCode.Binary), commandInfo.PreparedStatementName), cancellationToken).ConfigureAwait(false);
                    await writer.WriteMessageAsync(new Execute(portal), cancellationToken).ConfigureAwait(false);
                    break;
                case CommandKind.Unprepared:
                    await writer.WriteMessageAsync(new Parse(commandInfo.CommandText!, commandInfo.Parameters, commandInfo.PreparedStatementName), cancellationToken).ConfigureAwait(false);
                    await writer.WriteMessageAsync(new Bind(portal, commandInfo.Parameters, ResultColumnCodes.CreateOverall(FormatCode.Binary), commandInfo.PreparedStatementName), cancellationToken).ConfigureAwait(false);
                    await writer.WriteMessageAsync(new Describe(DescribeName.CreateForPortal(portal)), cancellationToken).ConfigureAwait(false);
                    await writer.WriteMessageAsync(new Execute(portal), cancellationToken).ConfigureAwait(false);
                    break;
            }

            if (commandInfo.AppendErrorBarrier)
                await writer.WriteMessageAsync(new Sync(), cancellationToken).ConfigureAwait(false);
        }, commandInfo, flushHint, cancellationToken);
    }
}

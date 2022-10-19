using System;
using System.Collections.Generic;
using System.Threading;

namespace Npgsql.Pipelines.Protocol.PgV3;

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
    public static IOCompletionPair WriteExtendedAsync(OperationSlot slot, ICommandInfo commandInfo, bool flushHint = true, CancellationToken cancellationToken = default)
    {
        commandInfo.Validate();
        if (slot.Protocol is not PgV3Protocol protocol)
            throw new ArgumentException($"Cannot write with a slot for a different protocol type, expected: {nameof(PgV3Protocol)}.", nameof(slot));

        return protocol.WriteMessageBatchAsync(slot, static async (writer, commandInfo, cancellationToken) =>
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

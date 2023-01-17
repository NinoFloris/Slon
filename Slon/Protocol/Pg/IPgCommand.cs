using System;

namespace Slon.Protocol.Pg;

[Flags]
enum CommandFlags : short
{
    None = 0,
    ErrorBarrier = 1,
}

static class CommandFlagsExtensions
{
    public static bool HasErrorBarrier(this CommandFlags flags) => (flags & CommandFlags.ErrorBarrier) != 0;
}

readonly struct CommandValues: IDisposable
{
    public required ParameterContext ParameterContext { get; init; }
    public required CommandFlags Flags { get; init; }
    public required RowRepresentation RowRepresentation { get; init; }
    public object? State { get; init; }

    public void Dispose() => ParameterContext.Dispose();
}

interface IPgCommand: ICommand<CommandValues, CommandExecution>
{
}


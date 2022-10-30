using System;
using System.Buffers;
using System.Collections.Generic;
using Npgsql.Pipelines.Protocol.PgV3.Commands; // TODO clean up Statement

namespace Npgsql.Pipelines.Protocol;

[Flags]
enum ExecutionFlags
{
    None = default,
    Unprepared = 1,
    Preparing = 2,
    Prepared = 4,
    ErrorBarrier = 8,
}

static class ExecutionFlagsExtensions
{
    public static bool HasUnprepared(this ExecutionFlags flags) => (flags & ExecutionFlags.Unprepared) != 0;
    public static bool HasErrorBarrier(this ExecutionFlags flags) => (flags & ExecutionFlags.ErrorBarrier) != 0;
    public static bool HasPreparing(this ExecutionFlags flags) => (flags & ExecutionFlags.Preparing) != 0;
    public static bool HasPrepared(this ExecutionFlags flags) => (flags & ExecutionFlags.Prepared) != 0;
}

interface IParameterWriter
{
    void Write<T>(ref BufferWriter<T> writer, in CommandParameter parameter) where T : IBufferWriter<byte>;
}

interface ICommand
{
    // The underlying values might change so we hand out a copy.
    Values GetValues();

    // Returns a session that is isolated from any outside mutation.
    public ICommandSession StartSession(in Values parameters);

    public readonly struct Values
    {
        public required ReadOnlyMemory<KeyValuePair<CommandParameter, IParameterWriter>> Parameters { get; init; }
        public required string StatementText { get; init; }
        public required ExecutionFlags ExecutionFlags { get; init; }
        public Statement? Statement { get; init; }
    }
}

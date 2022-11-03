using System;
using System.Buffers;
using System.Collections.Generic;
using Npgsql.Pipelines.Protocol.PgV3.Commands; // TODO clean up Statement

namespace Npgsql.Pipelines.Protocol;

[Flags]
enum ExecutionFlags
{
    Unprepared = 64,
    Preparing = 128,
    Prepared = 256,
    ErrorBarrier = 512, // Can be combined with one of the three previous flags.

    // Command Behavior flags, these are currently mapped to the same integers, otherwise change extension method CommandBehavior.ToExecutionFlags.
    Default = 0,
    SingleResult = 1,  // with data, force single result, may affect database
    SchemaOnly = 2,  // column info, no data, no effect on database
    KeyInfo = 4,  // column info + primary key information (if available)
    SingleRow = 8, // data, hint single row and single result, may affect database - doesn't apply to child(chapter) results
    SequentialAccess = 16,
    CloseConnection = 32
}

static class ExecutionFlagsExtensions
{
    public static bool HasUnprepared(this ExecutionFlags flags) => (flags & ExecutionFlags.Unprepared) == ExecutionFlags.Unprepared;
    public static bool HasErrorBarrier(this ExecutionFlags flags) => (flags & ExecutionFlags.ErrorBarrier) == ExecutionFlags.ErrorBarrier;
    public static bool HasPreparing(this ExecutionFlags flags) => (flags & ExecutionFlags.Preparing) == ExecutionFlags.Preparing;
    public static bool HasPrepared(this ExecutionFlags flags) => (flags & ExecutionFlags.Prepared) == ExecutionFlags.Prepared;
}

interface IParameterWriter
{
    void Write<T>(ref BufferWriter<T> writer, CommandParameter parameter) where T : IBufferWriter<byte>;
    void Write<T>(ref SpanBufferWriter<T> writer, CommandParameter parameter) where T : IBufferWriter<byte>;
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

using System;
using System.Diagnostics;
using Slon.Pg;

namespace Slon.Protocol.Pg;

[Flags]
enum ParameterContextFlags: short
{
    None = 0,
    AnyUnknownByteCount = 1,
    AnyUpperBoundByteCount = 2,
    AnyNonBinaryWrites = 4,
    AnySessions = 8,
    AnyWritableParamSessions = 16,
    AnyWriteState = 32,
}

// Parameters have sensitive disposal semantics so we don't try to handle that in this Dispose.
readonly struct ParameterContext: IDisposable
{
    // TODO ValidateFlags() maybe in the constructor or translated to tests of ParameterContextBuilder.

    public PooledMemory<Parameter> Parameters { get; init; }
    public ParameterContextFlags Flags { get; init; }
    internal int? MinimalBufferSegmentSize { get; init; }

    public void Dispose()
    {
        Parameters.Dispose();
    }

    public static ParameterContext Empty => new() { Parameters = PooledMemory<Parameter>.Empty };
}

static class ParameterContextExtensions
{
    [Conditional("DEBUG")]
    public static void ValidateFlags(this ParameterContext context)
    {
        var hasUpperBounds = context.HasUpperBoundByteCounts();
        var hasUnknowns = context.HasUnknownByteCounts();
        // Make sure these two flags are not set together, would point to an issue in ParameterContextBuilder.
        Debug.Assert(!(hasUpperBounds && hasUnknowns));

        if (hasUpperBounds)
        {
            // Validate AnyUpperBoundByteCount means there is at least one parameter with that flag.
            // Additionally validate at the parameter level there is no mixing of UpperBound and Unknown.
            var anyUpperBoundParam = false;
            foreach (var p in context.Parameters.Span)
            {
                Debug.Assert(p.Size?.Kind is not SizeResultKind.Unknown);
                if (p.Size?.Kind is SizeResultKind.UpperBound)
                    anyUpperBoundParam = true;
            }

            Debug.Assert(anyUpperBoundParam);
        }
        else if (hasUnknowns)
        {
            // Validate AnyUnknownByteCount means there is at least one parameter with that flag.
            // Additionally validate at the parameter level there is no mixing of UpperBound and Unknown.
            var anyUnknownParam = false;
            foreach (var p in context.Parameters.Span)
            {
                Debug.Assert(p.Size?.Kind is not SizeResultKind.UpperBound);
                if (p.Size?.Kind is SizeResultKind.Unknown)
                    anyUnknownParam = true;
            }
            Debug.Assert(anyUnknownParam);
        }

        if (context.HasTextWrites())
        {
            // Validate AnyUnknownByteCount means there is at least one parameter with that flag.
            // Additionally validate at the parameter level there is no mixing of UpperBound and Unknown.
            var anyTextWrite = false;
            foreach (var p in context.Parameters.Span)
            {
                if (p.HasTextWrite())
                {
                    anyTextWrite = true;
                    break;
                }
            }
            Debug.Assert(anyTextWrite);
        }

        // TODO validate the sessions/outputsessions/writestate
    }

    public static bool HasWriteState(this ParameterContext context)
        => (context.Flags & ParameterContextFlags.AnyWriteState) != 0;

    public static bool HasSessions(this ParameterContext context)
        => (context.Flags & ParameterContextFlags.AnySessions) != 0;

    public static bool HasWritableParamSessions(this ParameterContext context)
        => (context.Flags & ParameterContextFlags.AnyWritableParamSessions) != 0;

    public static bool HasTextWrites(this ParameterContext context)
        => (context.Flags & ParameterContextFlags.AnyNonBinaryWrites) != 0;

    public static bool HasUpperBoundByteCounts(this ParameterContext context)
        => (context.Flags & ParameterContextFlags.AnyUpperBoundByteCount) != 0;

    public static bool HasUnknownByteCounts(this ParameterContext context)
        => (context.Flags & ParameterContextFlags.AnyUnknownByteCount) != 0;
}

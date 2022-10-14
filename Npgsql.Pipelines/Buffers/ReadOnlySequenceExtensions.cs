using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Npgsql.Pipelines.Buffers;

static class ReadOnlySequenceExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<T> GetFirstSpan<T>(this in ReadOnlySequence<T> buffer)
    {
#if !NETSTANDARD2_0
        return buffer.FirstSpan;
#else
        return buffer.First.Span;
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryCopyTo<T>(this in ReadOnlySequence<T> buffer, Span<T> destination)
    {
        var firstSpan = buffer.First.Span;
        if (firstSpan.Length >= destination.Length)
        {
            firstSpan.Slice(0, destination.Length).CopyTo(destination);
            return true;
        }

        return TryCopySlow(buffer, destination);
    }

    public static bool TryCopySlow<T>(in ReadOnlySequence<T> buffer, Span<T> destination)
    {
        if (buffer.Length < destination.Length)
            return false;

        var copied = 0;
        var segment = buffer.First;
        var next = buffer.GetPosition(segment.Length);
        do
        {
            if (segment.Length > 0)
            {
                var segmentSpan = segment.Span;
                var toCopy = Math.Min(segmentSpan.Length, destination.Length - copied);
                segmentSpan.Slice(0, toCopy).CopyTo(destination.Slice(copied));
                copied += toCopy;
            }
        } while (copied < destination.Length && buffer.TryGet(ref next, out segment, true));

        return true;
    }
}

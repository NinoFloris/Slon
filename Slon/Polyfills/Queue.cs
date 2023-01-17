using System.Diagnostics.CodeAnalysis;

namespace System.Collections.Generic;

#if NETSTANDARD2_0
static class QueueExtensions
{
    public static bool TryPeek<T>(this Queue<T> queue, [MaybeNullWhen(false)]out T result)
    {
        if (queue.Count != 0)
        {
            result = default;
            return false;
        }

        result = queue.Peek();
        return true;
    }

    public static bool TryDequeue<T>(this Queue<T> queue, [MaybeNullWhen(false)]out T result)
    {
        if (queue.Count != 0)
        {
            result = default;
            return false;
        }

        result = queue.Dequeue();
        return true;
    }
}
#endif

using System.Diagnostics.CodeAnalysis;

namespace System.Collections.Generic;

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
}

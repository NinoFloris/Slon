using System;

namespace Slon.Pg.Converters;

#if NETSTANDARD2_0
static class TypeSupport
{
    public static bool IsSupported(Type[] supportedTypes, Type actualType)
    {
        var anySupported = false;
        foreach (var t in supportedTypes)
            if (t == actualType)
            {
                anySupported = true;
                break;
            }

        return anySupported;
    }

    public static void ThrowIfNotSupported(Type[] supportedTypes, Type actualType)
    {
        if (!IsSupported(supportedTypes, actualType))
            ThrowNotSupported(actualType);

        static void ThrowNotSupported(Type actualType) => throw new NotSupportedException($"T instantiated to {actualType} is not supported.");
    }
}
#endif

using System;
using System.Runtime.CompilerServices;

namespace Npgsql.Pipelines;

static class ConvertShim
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ToHexString(ReadOnlySpan<byte> bytes)
    {
#if NETSTANDARD2_0
        return HexConverter.ToString(bytes);
#else
        return System.Convert.ToHexString(bytes);
#endif
    }
}

static class EnumShim
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsDefined<T>(T value) where T : struct, System.Enum
    {
#if NETSTANDARD2_0
        return System.Enum.IsDefined(typeof(T), value);
#else
    return System.Enum.IsDefined(value);
#endif
    }
}

static class UnsafeShim
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SkipInit<T>(out T value)
    {
#if NETSTANDARD2_0
        value = default!;
#else
        Unsafe.SkipInit(out value);
#endif
    }
}

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Npgsql.Pipelines;

// See https://github.com/dotnet/runtime/issues/65184
static class InterlockedShim
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe T Exchange<T>(ref T location, T value) where T: unmanaged
        => sizeof(T) switch
        {
            sizeof(int) => (T)(object)Interlocked.Exchange(ref Unsafe.As<T, int>(ref location), Unsafe.As<T, int>(ref value)),
            _ => throw new NotSupportedException("This type cannot be handled atomically")
        };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe T CompareExchange<T>(ref T location, T value, T comparand) where T: unmanaged
        => sizeof(T) switch
        {
            sizeof(int) => (T)(object)Interlocked.CompareExchange(ref Unsafe.As<T, int>(ref location), Unsafe.As<T, int>(ref value), Unsafe.As<T, int>(ref comparand)),
            _ => throw new NotSupportedException("This type cannot be handled atomically")
        };
}

static class DebugShim
{
    // Debug.Assert that is annotated for ns2.0 to take nullability into account.
    [Conditional("DEBUG")]
    public static void Assert([DoesNotReturnIf(false)] bool condition)
        => Debug.Assert(condition);

    // Debug.Assert that is annotated for ns2.0 to take nullability into account.
    [Conditional("DEBUG")]
    public static void Assert([DoesNotReturnIf(false)] bool condition, string message)
        => Debug.Assert(condition, message);
}

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

static class TickCount64Shim
{
    /// Gets the number of milliseconds elapsed since the system started.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Get()
    {
    #if NETSTANDARD2_0
        return Stopwatch.GetTimestamp() / (Stopwatch.Frequency / 1000);
#else
        return Environment.TickCount64;
#endif
    }
}

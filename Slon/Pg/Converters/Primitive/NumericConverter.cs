using System;
using System.Buffers;
using System.Numerics;
using System.Runtime.InteropServices;
using Slon.Pg.Types;

namespace Slon.Pg.Converters;

file static class NumericConverter
{
#if NETSTANDARD2_0
    public static Type[] SupportedTypes => new[]
    {
        typeof(short),
        typeof(int),
        typeof(long),
        typeof(byte),
        typeof(sbyte),
        typeof(float),
        typeof(double),
        typeof(decimal),
        typeof(BigInteger)
    };
#endif
    public static PgNumeric.Builder Read(PgReader reader, Span<short> digits)
    {
        // TODO We could ask for a span from the buffer if everything is buffered instead of doing a copy.
        var weight = reader.ReadInt16();
        var sign = reader.ReadInt16();
        var scale = reader.ReadInt16();
        foreach (ref var digit in digits)
            digit = reader.ReadInt16();

        return new PgNumeric.Builder(digits, weight, sign, scale);
    }

    public static void Write(PgWriter writer, PgNumeric.Builder numeric)
    {
        writer.WriteInt16((short)numeric.Digits.Length);
        writer.WriteInt16(numeric.Weight);
        writer.WriteInt16(numeric.Sign);
        writer.WriteInt16(numeric.Scale);

        foreach (var digit in numeric.Digits)
            writer.WriteInt16(digit);
    }
}

sealed class NumericConverter<T> : PgBufferedConverter<T>
#if !NETSTANDARD2_0
    where T : INumberBase<T>
#endif
{
#if NETSTANDARD2_0
    static NumericConverter() => TypeSupport.ThrowIfNotSupported(NumericConverter.SupportedTypes, typeof(T));
#endif
    const int StackAllocByteThreshold = 64 * sizeof(uint);

    public override ValueSize GetSize(ref SizeContext context, T value) =>
        PgNumeric.GetByteCount(default(T) switch
        {
            _ when typeof(decimal) == typeof(T) => PgNumeric.GetDigitCount((decimal)(object)value!),
            _ when typeof(BigInteger) == typeof(T) => PgNumeric.GetDigitCount((BigInteger)(object)value!),
            _ => throw new NotSupportedException()
        });

    protected override T ReadCore(PgReader reader)
    {
        var digitCount = reader.ReadInt16();
        byte[]? digitsFromPool = null;
        var digits = digitCount <= StackAllocByteThreshold / sizeof(short)
            ? stackalloc short[StackAllocByteThreshold / sizeof(short)]
            : MemoryMarshal.Cast<byte, short>(digitsFromPool = ArrayPool<byte>.Shared.Rent(digitCount * sizeof(short))).Slice(0, digitCount);

        var value = ConvertTo(NumericConverter.Read(reader, digits));

        if (digitsFromPool is not null)
            ArrayPool<byte>.Shared.Return(digitsFromPool);

        return value;
    }

    public override void Write(PgWriter writer, T value)
    {
        // We don't know how many digits we need so we allocate a decent chunk of stack for the builder to use.
        // If it's not enough for the BigInteger the builder will do a heap allocation.
        Span<short> destination =
            typeof(BigInteger) == typeof(T)
                ? stackalloc short[StackAllocByteThreshold / sizeof(short)]
                : stackalloc short[PgNumeric.Builder.MaxDecimalNumericDigits];

        var numeric = ConvertFrom(value, destination);
        NumericConverter.Write(writer, numeric);
    }

    PgNumeric.Builder ConvertFrom(T value, Span<short> destination)
    {
        if (typeof(BigInteger) == typeof(T))
            return new PgNumeric.Builder((BigInteger)(object)value!, destination);

#if NETSTANDARD2_0
        if (typeof(short) == typeof(T))
            return new PgNumeric.Builder((decimal)(short)(object)value!, destination);
        if (typeof(int) == typeof(T))
            return new PgNumeric.Builder((decimal)(int)(object)value!, destination);
        if (typeof(long) == typeof(T))
            return new PgNumeric.Builder((decimal)(long)(object)value!, destination);

        if (typeof(byte) == typeof(T))
            return new PgNumeric.Builder((decimal)(byte)(object)value!, destination);
        if (typeof(sbyte) == typeof(T))
            return new PgNumeric.Builder((decimal)(sbyte)(object)value!, destination);

        if (typeof(float) == typeof(T))
            return new PgNumeric.Builder((decimal)(float)(object)value!, destination);
        if (typeof(double) == typeof(T))
            return new PgNumeric.Builder((decimal)(double)(object)value!, destination);
        if (typeof(decimal) == typeof(T))
            return new PgNumeric.Builder((decimal)(object)value!, destination);

        throw new InvalidCastException();
#else
        return new PgNumeric.Builder(decimal.CreateChecked(value), destination);
#endif
    }

    T ConvertTo(in PgNumeric.Builder numeric)
    {
        if (typeof(BigInteger) == typeof(T))
            return (T)(object)numeric.ToBigInteger();

#if NETSTANDARD2_0
        if (typeof(short) == typeof(T))
            return (T)(object)(short)numeric.ToDecimal();
        if (typeof(int) == typeof(T))
            return (T)(object)(int)numeric.ToDecimal();
        if (typeof(long) == typeof(T))
            return (T)(object)(long)numeric.ToDecimal();

        if (typeof(byte) == typeof(T))
            return (T)(object)(byte)numeric.ToDecimal();
        if (typeof(sbyte) == typeof(T))
            return (T)(object)(sbyte)numeric.ToDecimal();

        if (typeof(float) == typeof(T))
            return (T)(object)(float)numeric.ToDecimal();
        if (typeof(double) == typeof(T))
            return (T)(object)(double)numeric.ToDecimal();
        if (typeof(decimal) == typeof(T))
            return (T)(object)numeric.ToDecimal();

        throw new InvalidCastException();
#else
        return T.CreateChecked(numeric.ToDecimal());
#endif
    }
}

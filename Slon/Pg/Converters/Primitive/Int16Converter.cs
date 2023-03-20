using System;
using System.Numerics;

namespace Slon.Pg.Converters;

#if NETSTANDARD2_0
static class Int16Converter
{
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
    };
}
#endif

sealed class Int16Converter<T> : PgBufferedConverter<T>
#if !NETSTANDARD2_0
    where T : struct, INumberBase<T>
#else
    where T : struct
#endif
{
#if NETSTANDARD2_0
    static Int16Converter() => TypeSupport.ThrowIfNotSupported(Int64Converter.SupportedTypes, typeof(T));
#endif

    public override ValueSize GetSize(ref SizeContext context, T value) => sizeof(short);

#if !NETSTANDARD2_0
    protected override T ReadCore(PgReader reader) => T.CreateChecked(reader.ReadInt16());
    public override void Write(PgWriter writer, T value) => writer.WriteInt16(short.CreateChecked(value));
#else
    protected override T ReadCore(PgReader reader)
    {
        var value = reader.ReadInt16();
        if (typeof(short) == typeof(T))
            return (T)(object)value;
        if (typeof(int) == typeof(T))
            return (T)(object)(int)value;
        if (typeof(long) == typeof(T))
            return (T)(object)(long)value;

        if (typeof(byte) == typeof(T))
            return (T)(object)checked((byte)value);
        if (typeof(sbyte) == typeof(T))
            return (T)(object)checked((sbyte)value);

        if (typeof(float) == typeof(T))
            return (T)(object)(float)value;
        if (typeof(double) == typeof(T))
            return (T)(object)(double)value;
        if (typeof(decimal) == typeof(T))
            return (T)(object)(decimal)value;

        throw new InvalidCastException();
    }

    public override void Write(PgWriter writer, T value)
    {
        if (typeof(short) == typeof(T))
            writer.WriteInt16((short)(object)value);
        else if (typeof(int) == typeof(T))
            writer.WriteInt16(checked((short)(int)(object)value));
        else if (typeof(long) == typeof(T))
            writer.WriteInt16(checked((short)(long)(object)value));

        else if (typeof(byte) == typeof(T))
            writer.WriteInt16((byte)(object)value);
        else if (typeof(sbyte) == typeof(T))
            writer.WriteInt16((sbyte)(object)value);

        else if (typeof(float) == typeof(T))
            writer.WriteInt16(checked((short)(float)(object)value));
        else if (typeof(double) == typeof(T))
            writer.WriteInt16(checked((short)(double)(object)value));
        else if (typeof(decimal) == typeof(T))
            writer.WriteInt16((short)(decimal)(object)value);
        else
            throw new InvalidCastException();
    }
#endif
}

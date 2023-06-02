using System;
using System.Numerics;

namespace Slon.Pg.Converters;

#if NETSTANDARD2_0
static class DoubleConverter
{
    public static Type[] SupportedTypes => new[]
    {
        typeof(double),
    };
}
#endif

sealed class DoubleConverter<T> : PgBufferedConverter<T>
#if !NETSTANDARD2_0
    where T : INumberBase<T>
#endif
{
#if NETSTANDARD2_0
    static DoubleConverter() => TypeSupport.ThrowIfNotSupported(DoubleConverter.SupportedTypes, typeof(T));
#endif

    public override ValueSize GetSize(ref SizeContext context, T value) => sizeof(double);

#if !NETSTANDARD2_0
    protected override T ReadCore(PgReader reader) => T.CreateChecked(reader.ReadDouble());
    public override void Write(PgWriter writer, T value) => writer.WriteFloat(float.CreateChecked(value));
#else
    protected override T ReadCore(PgReader reader)
    {
        var value = reader.ReadDouble();
        if (typeof(double) == typeof(T))
            return (T)(object)(double)value;

        throw new InvalidCastException();
    }

    public override void Write(PgWriter writer, T value)
    {
        if (typeof(float) == typeof(T))
            writer.WriteDouble((double)(object)value!);
        else
            throw new InvalidCastException();
    }
#endif
}

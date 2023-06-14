using System;
using System.Numerics;

namespace Slon.Pg.Converters;

#if NETSTANDARD2_0
static class RealConverter
{
    public static Type[] SupportedTypes => new[]
    {
        typeof(float),
        typeof(double),
    };
}
#endif

sealed class RealConverter<T> : PgBufferedConverter<T>
#if !NETSTANDARD2_0
    where T : INumberBase<T>
#endif
{
#if NETSTANDARD2_0
    static RealConverter() => TypeSupport.ThrowIfNotSupported(RealConverter.SupportedTypes, typeof(T));
#endif

    public override ValueSize GetSize(ref SizeContext context, T value) => sizeof(float);

#if !NETSTANDARD2_0
    protected override T ReadCore(PgReader reader) => T.CreateChecked(reader.ReadFloat());
    public override void Write(PgWriter writer, T value) => writer.WriteFloat(float.CreateChecked(value));
#else
    protected override T ReadCore(PgReader reader)
    {
        var value = reader.ReadFloat();
        if (typeof(float) == typeof(T))
            return (T)(object)value;
        if (typeof(double) == typeof(T))
            return (T)(object)(double)value;

        throw new InvalidCastException();
    }

    public override void Write(PgWriter writer, T value)
    {
        if (typeof(float) == typeof(T))
            writer.WriteFloat((float)(object)value!);
        else if (typeof(double) == typeof(T))
            writer.WriteFloat((float)(double)(object)value!);
        else
            throw new InvalidCastException();
    }
#endif
}

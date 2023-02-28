using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Types;
using Slon.Protocol;

namespace Slon.Pg;

// TODO should GetConverter and GetPgTypeId get options passed in or should it always just be instance state if necessary?

abstract class PgConverterResolver : PgConverter
{
    internal PgConverterResolver(Type type) => Type = type;

    public Type Type { get; }
    public abstract PgConverter GetConverterAsObject(object? value);

    // The only implementation of GetConverterAsObject is PgConverterResolver<T> which does the checks.
    internal PgConverter GetConverterAsObjectInternal(object? value)
        => GetConverterAsObject(value);

    public abstract PgTypeId GetDataTypeNameAsObject(object? value);

    internal sealed override bool IsDbNullValueAsObject(object? value, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override SizeResult GetSizeAsObject(object value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override ReadStatus ReadAsObject(ref SequenceReader<byte> reader, int byteCount, out object? value, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override void WriteAsObject(PgWriter writer, object? value, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override ValueTask WriteAsObjectAsync(PgWriter writer, object? value, PgConverterOptions options, CancellationToken cancellationToken = default) => throw new NotSupportedException();
}

abstract class PgConverterResolver<T> : PgConverterResolver
{
    protected PgConverterResolver() : base(typeof(T))
    {
    }

    public sealed override PgConverter GetConverterAsObject(object? value) => GetConverter((T?)value);

    /// <summary>
    /// Gets a converter based on the given value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns>The converter.</returns>
    /// <remarks>
    /// Implementations should not return new instances of the possible converters that can be returned, instead its expected these are cached once used.
    /// Array or other collection converters depend on this to cache their own converter - which wraps the element converter - with the cache key being the element converter reference.
    /// </remarks>
    public abstract PgConverter<T> GetConverter(T? value);

    internal PgConverter<T> GetConverterInternal(T? value)
        => GetConverter(value) switch
        {
            null => throw new InvalidOperationException($"{nameof(GetConverter)} returned null unexpectedly."),
            var c => c
        };


    public sealed override PgTypeId GetDataTypeNameAsObject(object? value) => GetDataTypeName((T?)value);

    /// Gets a pg type id based on the given value.
    public abstract PgTypeId GetDataTypeName(T? value);
}

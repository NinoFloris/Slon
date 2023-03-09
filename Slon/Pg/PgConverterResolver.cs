using System;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg;

// TODO should GetConverter and GetPgTypeId get options passed in or should it always just be instance state if necessary?

abstract class PgConverterResolver
{
    internal PgConverterResolver() {}

    internal abstract PgConverter GetConverterAsObject(Field field);
    internal abstract PgConverter GetConverterAsObject(object? value);
    internal abstract PgTypeId GetPgTypeIdAsObject(object? value);
}

abstract class PgConverterResolver<T> : PgConverterResolver
{
    /// <summary>
    /// Gets the appropriate converter to write with based on the given value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns>The converter.</returns>
    /// <remarks>
    /// Implementations should not return new instances of the possible converters that can be returned, instead its expected these are cached once used.
    /// Array or other collection converters depend on this to cache their own converter - which wraps the element converter - with the cache key being the element converter reference.
    /// </remarks>
    public abstract PgConverter<T> GetConverter(T? value);

    /// <summary>
    /// Gets the appropriate converter to read with based on the given field info.
    /// </summary>
    /// <param name="field"></param>
    /// <returns>The converter.</returns>
    /// <remarks>
    /// Implementations should not return new instances of the possible converters that can be returned, instead its expected these are cached once used.
    /// Array or other collection converters depend on this to cache their own converter - which wraps the element converter - with the cache key being the element converter reference.
    /// </remarks>
    public abstract PgConverter<T> GetConverter(Field field);

    /// Gets a pg type id based on the given value.
    public abstract PgTypeId GetPgTypeId(T? value);

    internal sealed override PgConverter GetConverterAsObject(Field field) => this.GetConverterInternal(GetConverter(field));
    internal sealed override PgConverter GetConverterAsObject(object? value) => this.GetConverterInternal(GetConverter((T?)value));
    internal sealed override PgTypeId GetPgTypeIdAsObject(object? value) => GetPgTypeId((T?)value);
}

static class PgConverterResolverExtensions
{
    internal static PgConverter<T> GetConverterInternal<T>(this PgConverterResolver<T> _, PgConverter<T>? converter)
        => converter switch
        {
            null => throw new InvalidOperationException($"{nameof(PgConverterResolver<T>.GetConverter)} returned null unexpectedly."),
            _ => converter
        };
}

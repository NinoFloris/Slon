using System;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg;

abstract class PgConverterResolver
{
    private protected PgConverterResolver() { }

    internal abstract Type TypeToConvert { get; }

    internal abstract PgConverterResolution GetDefaultAsObject(PgTypeId expectedPgTypeId, bool requirePortableIds);
    internal abstract PgConverterResolution GetAsObject(Field field, bool requirePortableIds);
    internal abstract PgConverterResolution GetAsObject(object? value, PgTypeId? expectedPgTypeId, bool requirePortableIds);

    private protected static void Validate(string methodName, PgConverter converter, PgTypeId pgTypeId, PgTypeId? expectedPgTypeId, bool requirePortableIds)
    {
        if (converter == null)
            throw new InvalidOperationException($"'{methodName}' returned a null {nameof(PgConverterResolution.Converter)} unexpectedly.");

        if (requirePortableIds && pgTypeId.IsOid || !requirePortableIds && pgTypeId.IsDataTypeName)
            throw new InvalidOperationException($"'{methodName}' returned a {nameof(PgConverterResolution.PgTypeId)} that was not in canonical form.");

        if (expectedPgTypeId is not null && pgTypeId != expectedPgTypeId)
            throw new InvalidOperationException(
                $"'{methodName}' returned a different {nameof(PgConverterResolution.PgTypeId)} than was passed in as expected." +
                $" If such a mismatch occurs an exception should be thrown instead.");
    }
}

abstract class PgConverterResolver<T> : PgConverterResolver
{
    /// <summary>
    /// Gets the appropriate converter solely based on PgTypeId.
    /// </summary>
    /// <param name="pgTypeId"></param>
    /// <returns>The converter resolution.</returns>
    /// <remarks>
    /// Implementations should not return new instances of the possible converters that can be returned, instead its expected these are cached once used.
    /// Array or other collection converters depend on this to cache their own converter - which wraps the element converter - with the cache key being the element converter reference.
    /// </remarks>
    public abstract PgConverterResolution<T> GetDefault(PgTypeId pgTypeId);

    /// <summary>
    /// Gets the appropriate converter to write with based on the given value.
    /// </summary>
    /// <param name="value"></param>
    /// <param name="expectedPgTypeId"></param>
    /// <returns>The converter resolution.</returns>
    /// <remarks>
    /// Implementations should not return new instances of the possible converters that can be returned, instead its expected these are cached once used.
    /// Array or other collection converters depend on this to cache their own converter - which wraps the element converter - with the cache key being the element converter reference.
    /// </remarks>
    public abstract PgConverterResolution<T> Get(T? value, PgTypeId? expectedPgTypeId);

    /// <summary>
    /// Gets the appropriate converter to read with based on the given field info.
    /// </summary>
    /// <param name="field"></param>
    /// <returns>The converter resolution.</returns>
    /// <remarks>
    /// Implementations should not return new instances of the possible converters that can be returned, instead its expected these are cached once used.
    /// Array or other collection converters depend on this to cache their own converter - which wraps the element converter - with the cache key being the element converter reference.
    /// </remarks>
    public virtual PgConverterResolution<T> Get(Field field) => GetDefault(field.PgTypeId);

    protected ArgumentOutOfRangeException CreateUnsupportedPgTypeIdException(PgTypeId pgTypeId)
        => new(nameof(pgTypeId), pgTypeId, "Unsupported PgTypeId.");

    internal sealed override Type TypeToConvert => typeof(T);

    internal PgConverterResolution<T> GetInternal(T? value, PgTypeId? expectedPgTypeId, bool requirePortableIds)
    {
        var resolution = Get(value, expectedPgTypeId);
        Validate(nameof(Get), resolution.Converter, resolution.PgTypeId, expectedPgTypeId, requirePortableIds);
        return resolution;
    }

    internal PgConverterResolution<T> GetInternal(Field field, bool requirePortableIds)
    {
        var resolution = Get(field);
        Validate(nameof(Get), resolution.Converter, resolution.PgTypeId, field.PgTypeId, requirePortableIds);
        return resolution;
    }

    internal sealed override PgConverterResolution GetDefaultAsObject(PgTypeId expectedPgTypeId, bool requirePortableIds)
    {
        var resolution = GetDefault(expectedPgTypeId);
        Validate(nameof(GetDefault), resolution.Converter, resolution.PgTypeId, expectedPgTypeId, requirePortableIds);
        return resolution.ToConverterResolution();
    }

    internal sealed override PgConverterResolution GetAsObject(Field field, bool requirePortableIds)
    {
        var resolution = Get(field);
        Validate(nameof(Get), resolution.Converter, resolution.PgTypeId, field.PgTypeId, requirePortableIds);
        return resolution.ToConverterResolution();
    }

    internal sealed override PgConverterResolution GetAsObject(object? value, PgTypeId? expectedPgTypeId, bool requirePortableIds)
    {
        var resolution = Get(value is null ? default : (T)value, expectedPgTypeId);
        Validate(nameof(Get), resolution.Converter, resolution.PgTypeId, expectedPgTypeId, requirePortableIds);
        return resolution.ToConverterResolution();
    }
}

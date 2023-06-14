using System;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg;

abstract class PgConverterResolver
{
    private protected PgConverterResolver() { }

    /// <summary>
    /// Gets the appropriate converter solely based on PgTypeId.
    /// </summary>
    /// <param name="pgTypeId"></param>
    /// <returns>The converter resolution.</returns>
    /// <remarks>
    /// Implementations should not return new instances of the possible converters that can be returned, instead its expected these are cached once used.
    /// Array or other collection converters depend on this to cache their own converter - which wraps the element converter - with the cache key being the element converter reference.
    /// </remarks>
    public abstract PgConverterResolution GetDefault(PgTypeId pgTypeId);

    /// <summary>
    /// Gets the appropriate converter to read with based on the given field info.
    /// </summary>
    /// <param name="field"></param>
    /// <returns>The converter resolution.</returns>
    /// <remarks>
    /// Implementations should not return new instances of the possible converters that can be returned, instead its expected these are cached once used.
    /// Array or other collection converters depend on this to cache their own converter - which wraps the element converter - with the cache key being the element converter reference.
    /// </remarks>
    public virtual PgConverterResolution Get(Field field) => GetDefault(field.PgTypeId);

    internal abstract Type TypeToConvert { get; }
    private protected abstract Type ConverterType { get; }

    internal abstract PgConverterResolution GetAsObjectInternal(object? value, PgTypeId? expectedPgTypeId, bool requirePortableIds, bool validate);

    internal PgConverterResolution GetDefaultInternal(PgTypeId pgTypeId, bool requirePortableIds, bool validate)
    {
        var resolution = GetDefault(pgTypeId);
        if (validate)
            Validate(nameof(GetDefault), resolution.Converter, ConverterType, resolution.PgTypeId, pgTypeId, requirePortableIds);
        return resolution;
    }

    internal PgConverterResolution GetInternal(Field field, bool requirePortableIds, bool validate)
    {
        var resolution = Get(field);
        if (validate)
            Validate(nameof(Get), resolution.Converter, ConverterType, resolution.PgTypeId, field.PgTypeId, requirePortableIds);
        return resolution;
    }

    private protected static void Validate(string methodName, PgConverter converter, Type expectedConverterType, PgTypeId pgTypeId, PgTypeId? expectedPgTypeId, bool requirePortableIds)
    {
        if (converter is null)
            throw new InvalidOperationException($"'{methodName}' returned a null {nameof(PgConverterResolution.Converter)} unexpectedly.");

        if (converter.GetType() != expectedConverterType)
            throw new InvalidOperationException($"'{methodName}' returned a {nameof(PgConverterResolution.Converter)} of type {converter.GetType()} instead of {expectedConverterType} unexpectedly.");

        if (requirePortableIds && pgTypeId.IsOid || !requirePortableIds && pgTypeId.IsDataTypeName)
            throw new InvalidOperationException($"'{methodName}' returned a {nameof(PgConverterResolution.PgTypeId)} that was not in canonical form.");

        if (expectedPgTypeId is not null && pgTypeId != expectedPgTypeId)
            throw new InvalidOperationException(
                $"'{methodName}' returned a different {nameof(PgConverterResolution.PgTypeId)} than was passed in as expected." +
                $" If such a mismatch occurs an exception should be thrown instead.");
    }

    protected ArgumentOutOfRangeException CreateUnsupportedPgTypeIdException(PgTypeId pgTypeId)
        => new(nameof(pgTypeId), pgTypeId, "Unsupported PgTypeId.");
}

abstract class PgConverterResolver<T> : PgConverterResolver
{
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
    public abstract PgConverterResolution Get(T? value, PgTypeId? expectedPgTypeId);

    internal sealed override Type TypeToConvert => typeof(T);
    private protected sealed override Type ConverterType => typeof(PgConverter<T>);

    internal PgConverterResolution GetInternal(T? value, PgTypeId? expectedPgTypeId, bool requirePortableIds, bool validate = true)
    {
        var resolution = Get(value, expectedPgTypeId);
        if (validate)
            Validate(nameof(Get), resolution.Converter, ConverterType, resolution.PgTypeId, expectedPgTypeId, requirePortableIds);
        return resolution;
    }

    internal sealed override PgConverterResolution GetAsObjectInternal(object? value, PgTypeId? expectedPgTypeId, bool requirePortableIds, bool validate)
    {
        var resolution = Get(value is null ? default : (T)value, expectedPgTypeId);
        if (validate)
            Validate(nameof(Get), resolution.Converter, ConverterType, resolution.PgTypeId, expectedPgTypeId, requirePortableIds);
        return resolution;
    }
}

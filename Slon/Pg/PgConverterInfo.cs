using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg;

class PgConverterInfo
{
    readonly bool _canBinaryConvert;
    readonly bool _canTextConvert;

    PgConverterInfo(PgConverterOptions options, Type? unboxedType, PgConverter converter, PgTypeId pgTypeId)
    {
        IsBoxing = unboxedType is not null && converter.TypeToConvert == typeof(object);
        Type = unboxedType ?? converter.TypeToConvert;
        Options = options;
        Converter = converter;
        PgTypeId = options.GetCanonicalTypeId(pgTypeId);
        _canBinaryConvert = converter.CanConvert(DataFormat.Binary);
        _canTextConvert = converter.CanConvert(DataFormat.Text);
    }

    PgConverterInfo(PgConverterOptions options, Type type, PgConverterResolution? resolution, bool isBoxing = false)
    {
        IsBoxing = isBoxing;
        Type = type;
        Options = options;
        if (resolution is { } res)
        {
            // Resolutions should always be in canonical form already.
            if (options.RequirePortableTypeIds && res.PgTypeId.IsOid || !options.RequirePortableTypeIds && res.PgTypeId.IsDataTypeName)
                throw new ArgumentException("Given type id is not in canonical form. Make sure ConverterResolver implementations close over canonical ids, e.g. by calling options.GetCanonicalTypeId(pgTypeId) on the constructor arguments.", nameof(PgTypeId));

            PgTypeId = res.PgTypeId;
            _canBinaryConvert = res.Converter.CanConvert(DataFormat.Binary);
            _canTextConvert = res.Converter.CanConvert(DataFormat.Text);
        }
    }

    bool HasCachedInfo => PgTypeId is not null;

    public Type Type { get; }
    public PgConverterOptions Options { get; }

    // Whether this ConverterInfo maps to the default CLR Type for the DataTypeName given to IPgConverterInfoResolver.GetConverterInfo.
    public bool IsDefault { get; private set; }
    public DataFormat? PreferredFormat { get; private set; }

    PgConverter? Converter { get; }
    [MemberNotNullWhen(false, nameof(Converter))]
    public bool IsValueDependent => Converter is null;

    // Only used for internal converters to save on binary bloat, we never hand out one to converters that don't support it.
    internal bool IsBoxing { get; }

    public PgTypeId? PgTypeId { get; }

    // Used for debugging, returns the resolver type for PgConverterResolverInfo instances.
    public Type ConverterType => IsValueDependent ? ((PgConverterResolverInfo)this).ConverterResolver.GetType() : Converter.GetType();

    // Having it here so we can easily extend any behavior.
    public void DisposeWriteState(object writeState)
    {
        if (writeState is IDisposable disposable)
            disposable.Dispose();
    }

    public PgConverterResolution<T> GetResolution<T>(T? value, PgTypeId? expectedPgTypeId = null) => GetResolutionCore(value, expectedPgTypeId, field: null);
    public PgConverterResolution GetResolutionAsObject(object? value, PgTypeId? expectedPgTypeId = null) => GetResolutionCore(value, expectedPgTypeId, field: null);

    public PgConverterResolution<T> GetResolution<T>(Field field) => GetResolutionCore<T>(field: field);
    public PgConverterResolution GetResolutionAsObject(Field field) => GetResolutionCore(field: field);

    PgConverterResolution<T> GetResolutionCore<T>(T? value = default, PgTypeId? expectedPgTypeId = null, Field? field = null)
    {
        PgConverterResolution<T> resolution;
        switch (this)
        {
            case { Converter: PgConverter<T> converterT }:
                resolution = new PgConverterResolution<T>(converterT, PgTypeId.GetValueOrDefault(), IsBoxing ? Type : null);
                break;
            case PgConverterResolverInfo { ConverterResolver: PgConverterResolver<T> resolverT }:
                resolution = field is null ? resolverT.GetInternal(value, expectedPgTypeId, Options.RequirePortableTypeIds) : resolverT.GetInternal(field.GetValueOrDefault(), Options.RequirePortableTypeIds);
                ThrowIfInvalidEffectiveType(resolution.EffectiveType);
                break;
            default:
                ThrowNotSupportedType(typeof(T));
                return default;
        }

        return resolution;
    }

    PgConverterResolution GetResolutionCore(object? value = default, PgTypeId? expectedPgTypeId = null, Field? field = null)
    {
        PgConverterResolution resolution;
        switch (this)
        {
            case { Converter: { } converter }:
                resolution = new(converter, PgTypeId.GetValueOrDefault(), IsBoxing ? Type : null);
                break;
            case PgConverterResolverInfo { ConverterResolver: { } resolver }:
                resolution = field is null
                    ? resolver.GetAsObject(value, expectedPgTypeId, Options.RequirePortableTypeIds)
                    : resolver.GetAsObject(field.GetValueOrDefault(), Options.RequirePortableTypeIds);
                ThrowIfInvalidEffectiveType(resolution.EffectiveType);
                break;
            default:
                ThrowNotSupportedType(Type);
                return default;
        }

        return resolution;
    }

    public ValueSize? GetPreferredSize<T>(PgConverterResolution<T> resolution, T? value, int bufferLength, out object? writeState, out DataFormat format, DataFormat? preferredFormat = null)
    {
        format = ResolvePreferredFormat(resolution.Converter, preferredFormat ?? PreferredFormat);
        var context = new SizeContext(format, bufferLength);
        if (resolution.Converter.IsDbNullValue(value))
        {
            writeState = null;
            return null;
        }
        var size = resolution.Converter.GetSize(ref context, value);
        writeState = context.WriteState;
        return size;
    }

    public ValueSize? GetPreferredSizeAsObject(PgConverterResolution resolution, object? value, int bufferLength, out object? writeState, out DataFormat format, DataFormat? preferredFormat = null)
    {
        format = ResolvePreferredFormat(resolution.Converter, preferredFormat ?? PreferredFormat);
        var context = new SizeContext(format, bufferLength);
        if (resolution.Converter.IsDbNullValueAsObject(value))
        {
            writeState = null;
            return null;
        }
        var size = resolution.Converter.GetSizeAsObject(ref context, value);
        writeState = context.WriteState;
        return size;
    }

    internal PgConverterInfo Compose(PgConverter converter, PgTypeId pgTypeId)
        => new(Options, null, converter, pgTypeId)
        {
            IsDefault = IsDefault,
            PreferredFormat = PreferredFormat
        };

    internal PgConverterInfo Compose(PgConverterResolver resolver, PgTypeId? expectedPgTypeId)
        => new PgConverterResolverInfo(Options, resolver, expectedPgTypeId)
        {
            IsDefault = IsDefault,
            PreferredFormat = PreferredFormat
        };

    internal static PgConverterInfo CreateBoxing(PgConverterOptions options, Type effectiveType, PgConverter converter, PgTypeId pgTypeId, bool isDefault = false, DataFormat? preferredFormat = null)
        => new(options, effectiveType, converter, pgTypeId) { IsDefault = isDefault, PreferredFormat = preferredFormat };

    internal static PgConverterInfo CreateBoxing(PgConverterOptions options, PgConverterResolver resolver, PgTypeId? expectedPgTypeId, bool isDefault = false, DataFormat? preferredFormat = null)
        => new PgConverterResolverInfo(options, resolver, expectedPgTypeId, isBoxing: true) { IsDefault = isDefault, PreferredFormat = preferredFormat };

    public static PgConverterInfo Create(PgConverterOptions options, PgConverter converter, PgTypeId pgTypeId, bool isDefault = false, DataFormat? preferredFormat = null)
        => new(options, null, converter, pgTypeId) { IsDefault = isDefault, PreferredFormat = preferredFormat };

    public static PgConverterInfo Create(PgConverterOptions options, PgConverterResolver resolver, PgTypeId? expectedPgTypeId, bool isDefault = false, DataFormat? preferredFormat = null)
        => new PgConverterResolverInfo(options, resolver, expectedPgTypeId) { IsDefault = isDefault, PreferredFormat = preferredFormat };

    sealed class PgConverterResolverInfo : PgConverterInfo
    {
        internal PgConverterResolverInfo(PgConverterOptions options, PgConverterResolver converterResolver, PgTypeId? pgTypeId, bool isBoxing = false)
            : base(options,
                converterResolver.TypeToConvert,
                pgTypeId is { } typeId ? converterResolver.GetDefaultAsObject(typeId, options.RequirePortableTypeIds) : null)
        {
            ConverterResolver = converterResolver;
        }

        public PgConverterResolver ConverterResolver { get; }
    }

    DataFormat ResolvePreferredFormat(PgConverter converter, DataFormat? preferredFormat = null)
        // If we don't have a converter stored we must ask the retrieved one through virtual calls.
        => preferredFormat switch
        {
            DataFormat.Binary when (HasCachedInfo ? _canBinaryConvert : converter.CanConvert(DataFormat.Binary))
                => DataFormat.Binary,
            DataFormat.Text when (HasCachedInfo ? !_canTextConvert : !converter.CanConvert(DataFormat.Text))
                => DataFormat.Binary,
            _ => (HasCachedInfo ? _canBinaryConvert : converter.CanConvert(DataFormat.Binary)) ? DataFormat.Binary : DataFormat.Text
        };

    [MethodImpl(MethodImplOptions.NoInlining)]
    void ThrowIfInvalidEffectiveType(Type actual)
    {
        if (IsBoxing && Type != actual)
            throw new InvalidOperationException($"{nameof(PgConverterResolution.EffectiveType)} for a boxing info can't be {actual} but must return {Type}");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void ThrowNotSupportedType(Type type) => throw new NotSupportedException(IsBoxing ? $"ConverterInfo only supports boxing conversions, call GetResolution<T> with {typeof(object)} instead of {type}." : null);
}

readonly struct PgConverterResolution
{
    readonly Type? _effectiveType;

    public PgConverterResolution(PgConverter converter, PgTypeId pgTypeId, Type? effectiveType = null)
    {
        DebugShim.Assert(effectiveType is null || converter.TypeToConvert == typeof(object), "effectiveType can only be set for object polymorphic converters.");
        Converter = converter;
        PgTypeId = pgTypeId;
        _effectiveType = effectiveType;
    }

    public PgConverter Converter { get; }
    public PgTypeId PgTypeId { get; }
    public Type EffectiveType => _effectiveType ?? Converter.TypeToConvert;

    public PgConverterResolution<T> ToConverterResolution<T>() => new((PgConverter<T>)Converter, PgTypeId);
}

readonly struct PgConverterResolution<T>
{
    readonly Type? _effectiveType;

    public PgConverterResolution(PgConverter<T> converter, PgTypeId pgTypeId, Type? effectiveType = null)
    {
        DebugShim.Assert(effectiveType is null || converter.TypeToConvert == typeof(object), "effectiveType can only be set for object polymorphic converters.");
        Converter = converter;
        PgTypeId = pgTypeId;
        _effectiveType = effectiveType;
    }

    public PgConverter<T> Converter { get; }
    public PgTypeId PgTypeId { get; }
    public Type EffectiveType => _effectiveType ?? Converter.TypeToConvert;

    public PgConverterResolution ToConverterResolution() => new(Converter, PgTypeId);
}

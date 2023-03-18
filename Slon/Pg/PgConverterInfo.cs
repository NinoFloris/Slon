using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg;

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

class PgConverterInfo
{
    readonly bool _canBinaryConvert;
    readonly bool _canTextConvert;

    PgConverterInfo(PgConverterOptions options, PgConverter converter, PgTypeId pgTypeId)
    {
        Type = converter.TypeToConvert;
        Options = options;
        Converter = converter;
        PgTypeId = options.GetCanonicalTypeId(pgTypeId);
        _canBinaryConvert = converter.CanConvert(DataFormat.Binary);
        _canTextConvert = converter.CanConvert(DataFormat.Text);
    }

    PgConverterInfo(PgConverterOptions options, Type type, PgConverterResolution? resolution)
    {
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

    PgConverterResolution<T> GetResolutionCore<T>(T? value = default, PgTypeId? expectedPgTypeId = null, Field? field = null)
    {
        switch (this)
        {
            case { Converter: PgConverter<T> converterT }:
                return new(converterT, PgTypeId.GetValueOrDefault());
            case PgConverterResolverInfo { ConverterResolver: PgConverterResolver<T> resolverT }:
                return field is null
                    ? resolverT.GetInternal(value, expectedPgTypeId, Options.RequirePortableTypeIds)
                    : resolverT.GetInternal(field.GetValueOrDefault(), Options.RequirePortableTypeIds);
            default:
                ThrowNotSupported();
                return default;
        }
    }

    PgConverterResolution GetResolutionCore(object? value = default, PgTypeId? expectedPgTypeId = null, Field? field = null)
    {
        switch (this)
        {
            case { Converter: { } converter }:
                return new(converter, PgTypeId.GetValueOrDefault());
            case PgConverterResolverInfo { ConverterResolver: { } resolver }:
                return field is null
                    ? resolver.GetAsObject(value, expectedPgTypeId, Options.RequirePortableTypeIds)
                    : resolver.GetAsObject(field.GetValueOrDefault(), Options.RequirePortableTypeIds);
            default:
                ThrowNotSupported();
                return default;
        }
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

    internal Reader<T> GetReader<T>(Field field) => new(GetResolutionCore<T>(field: field), this);
    internal Reader GetReader(Field field) => new(GetResolutionCore(field: field), this);

    internal PgConverterInfo Compose(PgConverter converter, PgTypeId pgTypeId)
        => new(Options, converter, pgTypeId)
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

    public static PgConverterInfo Create(PgConverterOptions options, PgConverter converter, PgTypeId pgTypeId, bool isDefault = false, DataFormat? preferredFormat = null)
        => new(options, converter, pgTypeId) { IsDefault = isDefault, PreferredFormat = preferredFormat };

    public static PgConverterInfo Create(PgConverterOptions options, PgConverterResolver resolver, PgTypeId? expectedPgTypeId, bool isDefault = false, DataFormat? preferredFormat = null)
        => new PgConverterResolverInfo(options, resolver, expectedPgTypeId) { IsDefault = isDefault, PreferredFormat = preferredFormat };

    internal readonly struct Reader<T>
    {
        readonly PgConverter<T> _converter;
        readonly PgConverterInfo _info;

        public Reader(PgConverterResolution<T> resolution, PgConverterInfo info)
        {
            _converter = resolution.Converter;
            _info = info;
            EffectiveType = resolution.EffectiveType;
        }

        public Type EffectiveType { get; }

        public bool IsDbNullValue(T? value) => _converter.IsDbNullValue(value);

        public T? Read(PgReader reader)
            => _converter.Read(reader);

        public ValueTask<T?> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
            => _converter.ReadAsync(reader, cancellationToken);
    }

    internal readonly struct Reader
    {
        readonly PgConverter _converter;
        readonly PgConverterInfo _info;

        public Reader(PgConverterResolution resolution, PgConverterInfo info)
        {
            _converter = resolution.Converter;
            _info = info;
            EffectiveType = resolution.EffectiveType;
        }

        public Type EffectiveType { get; }

        public object? Read(PgReader reader)
            => _converter.ReadAsObject(reader);

        public ValueTask<object?> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
            => _converter.ReadAsObjectAsync(reader, cancellationToken);
    }

    sealed class PgConverterResolverInfo : PgConverterInfo
    {
        internal PgConverterResolverInfo(PgConverterOptions options, PgConverterResolver converterResolver, PgTypeId? pgTypeId)
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

    void ThrowNotSupported() => throw new NotSupportedException();
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

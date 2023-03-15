using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg;

class PgConverterInfo
{
    readonly bool _canBinaryConvert;
    readonly bool _canTextConvert;
    readonly bool _isTypeDbNullable;

    PgConverterInfo(PgConverterOptions options, PgConverter converter, PgTypeId pgTypeId)
    {
        Type = converter.TypeToConvert;
        Options = options;
        Converter = converter;
        PgTypeId = options.GetCanonicalTypeId(pgTypeId);
        _canBinaryConvert = converter.CanConvert(DataRepresentation.Binary);
        _canTextConvert = converter.CanConvert(DataRepresentation.Text);
        _isTypeDbNullable = converter.IsDbNullable;
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
            _canBinaryConvert = res.Converter.CanConvert(DataRepresentation.Binary);
            _canTextConvert = res.Converter.CanConvert(DataRepresentation.Text);
            _isTypeDbNullable = res.Converter.IsDbNullable;
        }
    }

    bool HasCachedInfo => PgTypeId is not null;

    public Type Type { get; }
    public PgConverterOptions Options { get; }

    // Whether this ConverterInfo maps to the default CLR Type for the DataTypeName given to IPgConverterInfoResolver.GetConverterInfo.
    public bool IsDefault { get; private set; }
    public DataRepresentation? PreferredRepresentation { get; private set; }

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
    public PgConverterResolution GetResolution(object? value, PgTypeId? expectedPgTypeId = null) => GetResolutionCore(value, expectedPgTypeId, field: null);

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

    internal Writer<T> GetWriter<T>(T? value) => new(GetResolutionCore(value, PgTypeId), this);
    internal Writer GetWriter(object? value) => new(GetResolutionCore(value, PgTypeId), this);

    internal Reader<T> GetReader<T>(Field field) => new(GetResolutionCore<T>(field: field), this);
    internal Reader GetReader(Field field) => new(GetResolutionCore(field: field), this);

    internal PgConverterInfo Compose(PgConverter converter, PgTypeId pgTypeId)
        => new(Options, converter, pgTypeId)
        {
            IsDefault = IsDefault,
            PreferredRepresentation = PreferredRepresentation
        };

    internal PgConverterInfo Compose(PgConverterResolver resolver, PgTypeId? expectedPgTypeId)
        => new PgConverterResolverInfo(Options, resolver, expectedPgTypeId)
        {
            IsDefault = IsDefault,
            PreferredRepresentation = PreferredRepresentation
        };

    public static PgConverterInfo Create(PgConverterOptions options, PgConverter converter, PgTypeId pgTypeId, bool isDefault = false, DataRepresentation? preferredRepresentation = null)
        => new(options, converter, pgTypeId) { IsDefault = isDefault, PreferredRepresentation = preferredRepresentation };

    public static PgConverterInfo Create(PgConverterOptions options, PgConverterResolver resolver, PgTypeId? expectedPgTypeId, bool isDefault = false, DataRepresentation? preferredRepresentation = null)
        => new PgConverterResolverInfo(options, resolver, expectedPgTypeId) { IsDefault = isDefault, PreferredRepresentation = preferredRepresentation };

    internal readonly struct Writer<T>
    {
        readonly PgConverter<T> _converter;
        readonly PgTypeId _pgTypeId;
        public PgConverterInfo Info { get; }

        public Writer(PgConverterResolution<T> resolution, PgConverterInfo info)
        {
            _converter = resolution.Converter;
            _pgTypeId = resolution.PgTypeId;
            Info = info;
        }

        public bool IsDbNullValue([NotNullWhen(false)]T? value)
            => Info is { HasCachedInfo: true, _isTypeDbNullable: false } || _converter.IsDbNullValue(value, Info.Options);

        public ValueSize GetAnySize(T value, int bufferLength, out object? writeState, out DataRepresentation representation, DataRepresentation? preferredRepresentation = null)
        {
            writeState = null;
            representation = Info.ResolvePreferredRepresentation(_converter, preferredRepresentation ?? Info.PreferredRepresentation);
            return _converter.GetSize(value!, ref writeState, new(representation, bufferLength), Info.Options);
        }

        public void Write(PgWriter pgWriter, T value)
            => _converter.Write(pgWriter, value, Info.Options);

        public ValueTask WriteAsync(PgWriter pgWriter, T value, CancellationToken cancellationToken = default)
            => _converter.WriteAsync(pgWriter, value, Info.Options, cancellationToken);

        public Writer ToWriter() => new(new(_converter, _pgTypeId), Info);
    }

    internal readonly struct Writer
    {
        readonly PgConverter _converter;
        readonly PgTypeId _pgTypeId;
        public PgConverterInfo Info { get; }

        public Writer(PgConverterResolution resolution, PgConverterInfo info)
        {
            _converter = resolution.Converter;
            _pgTypeId = resolution.PgTypeId;
            Info = info;
        }

        public bool IsDbNullValue([NotNullWhen(false)]object? value)
            => (Info.HasCachedInfo || Info._isTypeDbNullable) && _converter.IsDbNullValueAsObject(value, Info.Options);

        public ValueSize GetAnySize(object value, int bufferLength, out object? writeState, out DataRepresentation representation, DataRepresentation? preferredRepresentation = null)
        {
            writeState = null;
            representation = Info.ResolvePreferredRepresentation(_converter, preferredRepresentation ?? Info.PreferredRepresentation);
            return _converter.GetSizeAsObject(value!, ref writeState, new(representation, bufferLength), Info.Options);
        }

        public void Write(PgWriter pgWriter, object value)
            => _converter.WriteAsObject(pgWriter, value, Info.Options);

        public ValueTask WriteAsync(PgWriter pgWriter, object value, CancellationToken cancellationToken)
            => _converter.WriteAsObjectAsync(pgWriter, value, Info.Options, cancellationToken);

        public Writer<T> ToWriter<T>() => new(new((PgConverter<T>)_converter, _pgTypeId), Info);
    }

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

        public T? Read(PgReader reader)
            => _converter.Read(reader, _info.Options);

        public ValueTask<T?> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
            => _converter.ReadAsync(reader, _info.Options, cancellationToken);
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
            => _converter.ReadAsObject(reader, _info.Options);

        public ValueTask<object?> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
            => _converter.ReadAsObjectAsync(reader, _info.Options, cancellationToken);
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

    DataRepresentation ResolvePreferredRepresentation(PgConverter converter, DataRepresentation? preferredRepresentation = null)
        // If we don't have a converter stored we must ask the retrieved one through virtual calls.
        => preferredRepresentation switch
        {
            DataRepresentation.Binary when (HasCachedInfo ? _canBinaryConvert : converter.CanConvert(DataRepresentation.Binary))
                => DataRepresentation.Binary,
            DataRepresentation.Text when (HasCachedInfo ? !_canTextConvert : !converter.CanConvert(DataRepresentation.Text))
                => DataRepresentation.Binary,
            _ => (HasCachedInfo ? _canBinaryConvert : converter.CanConvert(DataRepresentation.Binary)) ? DataRepresentation.Binary : DataRepresentation.Text
        };

    void ThrowNotSupported() => throw new NotSupportedException();
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

readonly struct PgConverterResolution
{
    readonly Type? _effectiveType;

    public PgConverterResolution(PgConverter converter, PgTypeId pgTypeId, Type? effectiveType = null)
    {
        DebugShim.Assert(effectiveType is null || converter.TypeToConvert == typeof(object));
        Converter = converter;
        PgTypeId = pgTypeId;
        _effectiveType = effectiveType;
    }

    public PgConverter Converter { get; }
    public PgTypeId PgTypeId { get; }
    public Type EffectiveType => _effectiveType ?? Converter.TypeToConvert;

    public PgConverterResolution<T> ToConverterResolution<T>() => new((PgConverter<T>)Converter, PgTypeId);
}

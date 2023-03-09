using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;
using Slon.Protocol;

namespace Slon.Pg;

class PgConverterInfo
{
    readonly bool _canBinaryConvert;
    readonly bool _canTextConvert;
    readonly bool _isTypeDbNullable;

    PgConverterInfo(PgConverterOptions options, Type type)
    {
        Type = type;
        Options = options;
    }

    PgConverterInfo(PgConverterOptions options, PgConverter converter, PgTypeId pgTypeId)
    {
        Type = converter.GetType().GenericTypeArguments[0];
        Options = options;
        Converter = converter;
        PgTypeId = options.GetCanonicalTypeId(pgTypeId);
        _canBinaryConvert = converter.CanConvert(DataRepresentation.Binary);
        _canTextConvert = converter.CanConvert(DataRepresentation.Text);
        _isTypeDbNullable = converter.IsDbNullable;
    }

    public Type Type { get; }
    public PgConverterOptions Options { get; }

    // Whether this ConverterInfo maps to the default CLR Type for the DataTypeName given to IPgConverterInfoResolver.GetConverterInfo.
    public bool IsDefault { get; private set; }
    public DataRepresentation? PreferredRepresentation { get; private set; }

    // null if this is a ConverterResolver instance.
    public PgTypeId? PgTypeId { get; }
    public PgConverter? Converter { get; }
    public bool IsValueDependent => Converter is null;

    // Used for debugging, returns the resolver type for PgConverterResolverInfo instances.
    public Type ConverterType => Converter is null ? ((PgConverterResolverInfo)this).ConverterResolver.GetType() : Converter.GetType();

    // Having it here so we can easily extend any behavior.
    public void DisposeWriteState(object writeState)
    {
        if (writeState is IDisposable disposable)
            disposable.Dispose();
    }

    public PgConverter<T> GetConverter<T>(T? value) => GetConverterCore(value, field: null);

    // Ugly combined method to reduce generic code bloat.
    PgConverter<T> GetConverterCore<T>(T? value = default, Field? field = null)
    {
        switch (this)
        {
            case { Converter: PgConverter<T> converterT }:
                return converterT;
            case PgConverterResolverInfo { ConverterResolver: PgConverterResolver<T> resolverT }:
                return resolverT.GetConverterInternal(field is null ? resolverT.GetConverter(value) : resolverT.GetConverter(field.GetValueOrDefault()));
            default:
                ThrowNotSupported();
                return null!;
        }
    }

    PgConverter GetConverterAsObject(object? value)
    {
        if (Converter is not null)
            return Converter;

        if (this is PgConverterResolverInfo resolverInfo)
            return resolverInfo.ConverterResolver.GetConverterAsObject(value);

        ThrowNotSupported();
        return null!;
    }

    public Writer<T> GetWriter<T>(T? value) => new(GetConverter(value), this);
    public Writer GetWriter(object? value) => new(GetConverterAsObject(value), this);

    public Reader<T> GetReader<T>(Field field) => new(GetConverterCore<T>(field: field), this);
    public Reader GetReader(Field field) => new(GetConverterAsObject(field), this);

    public PgTypeId GetPgTypeId(object? value)
    {
        if (PgTypeId is { } pgTypeId)
            return pgTypeId;

        if (this is PgConverterResolverInfo resolverInfo)
            return Options.GetCanonicalTypeId(resolverInfo.ConverterResolver.GetPgTypeIdAsObject(value));

        ThrowNotSupported();
        return default;
    }

    public PgTypeId GetPgTypeId<T>(T? value)
    {
        if (PgTypeId is { } pgTypeId)
            return pgTypeId;

        if (this is PgConverterResolverInfo { ConverterResolver: PgConverterResolver<T> resolverT })
            return Options.GetCanonicalTypeId(resolverT.GetPgTypeId(value));

        ThrowNotSupported();
        return default;
    }

    public PgConverterInfo ComposeDynamic(PgConverter converter, PgTypeId pgTypeId)
    {
        var info = (PgConverterInfo)Activator.CreateInstance(typeof(PgConverterInfo), Options, converter, pgTypeId)!;
        info.IsDefault = IsDefault;
        info.PreferredRepresentation = PreferredRepresentation;
        return info;
    }

    public PgConverterInfo ComposeDynamic(PgConverterResolver resolver)
    {
        var info = (PgConverterInfo)Activator.CreateInstance(typeof(PgConverterResolverInfo), Options, resolver)!;
        info.IsDefault = IsDefault;
        info.PreferredRepresentation = PreferredRepresentation;
        return info;
    }

    public static PgConverterInfo Create(PgConverterOptions options, PgConverter converter, PgTypeId pgTypeId, bool isDefault = false, DataRepresentation? preferredRepresentation = null)
        => new(options, converter, pgTypeId) { IsDefault = isDefault, PreferredRepresentation = preferredRepresentation };

    public static PgConverterInfo Create(PgConverterOptions options, PgConverterResolver resolver, bool isDefault = false, DataRepresentation? preferredRepresentation = null)
        => new PgConverterResolverInfo(options, resolver) { IsDefault = isDefault, PreferredRepresentation = preferredRepresentation };

    public readonly struct Writer<T>
    {
        readonly PgConverterInfo _info;
        readonly PgConverter<T> _converter;

        public Writer(PgConverter<T> converter, PgConverterInfo info)
        {
            _converter = converter;
            _info = info;
        }

        public bool IsDbNullValue([NotNullWhen(false)]T? value)
            => (_info.Converter is null || _info._isTypeDbNullable) && _converter.IsDbNullValue(value, _info.Options);

        public SizeResult GetAnySize(T value, int bufferLength, out object? writeState, out DataRepresentation representation, DataRepresentation? preferredRepresentation = null)
        {
            writeState = null;
            representation = _info.ResolvePreferredRepresentation(_converter, preferredRepresentation ?? _info.PreferredRepresentation);
            return _converter.GetSize(value!, bufferLength, ref writeState, representation, _info.Options);
        }

        public void Write(PgWriter pgWriter, T value)
            => _converter.Write(pgWriter, value, _info.Options);

        public ValueTask WriteAsync(PgWriter pgWriter, T value, CancellationToken cancellationToken = default)
            => _converter.WriteAsync(pgWriter, value, _info.Options, cancellationToken);
    }

    public readonly struct Writer
    {
        readonly PgConverterInfo _info;
        readonly PgConverter _converter;

        public Writer(PgConverter converter, PgConverterInfo info)
        {
            _converter = converter;
            _info = info;
        }

        public bool IsDbNullValue([NotNullWhen(false)]object? value)
            => (_info.Converter is null || _info._isTypeDbNullable) && _converter.IsDbNullValueAsObject(value, _info.Options);

        public SizeResult GetAnySize(object value, int bufferLength, out object? writeState, out DataRepresentation representation, DataRepresentation? preferredRepresentation = null)
        {
            writeState = null;
            representation = _info.ResolvePreferredRepresentation(_converter, preferredRepresentation ?? _info.PreferredRepresentation);
            return _converter.GetSizeAsObject(value!, bufferLength, ref writeState, representation, _info.Options);
        }

        public void Write(PgWriter pgWriter, object value)
            => _converter.WriteAsObject(pgWriter, value, _info.Options);

        public ValueTask WriteAsync(PgWriter pgWriter, object value, CancellationToken cancellationToken)
            => _converter.WriteAsObjectAsync(pgWriter, value, _info.Options, cancellationToken);
    }

    public readonly struct Reader<T>
    {
        readonly PgConverterInfo _info;
        readonly PgConverter<T> _converter;

        public Reader(PgConverter<T> converter, PgConverterInfo info)
        {
            _converter = converter;
            _info = info;
        }

        public ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out T? value)
            => _converter.Read(ref reader, byteCount, out value, _info.Options);
    }

    public readonly struct Reader
    {
        readonly PgConverterInfo _info;
        readonly PgConverter _converter;

        public Reader(PgConverter converter, PgConverterInfo info)
        {
            _converter = converter;
            _info = info;
        }

        public ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out object? value)
            => _converter.ReadAsObject(ref reader, byteCount, out value, _info.Options);
    }

    sealed class PgConverterResolverInfo : PgConverterInfo
    {
        internal PgConverterResolverInfo(PgConverterOptions options, PgConverterResolver converterResolver)
            : base(options, converterResolver.GetType().GenericTypeArguments[0])
        {
            ConverterResolver = converterResolver;
        }

        public PgConverterResolver ConverterResolver { get; }
    }

    DataRepresentation ResolvePreferredRepresentation(PgConverter converter, DataRepresentation? preferredRepresentation = null)
        // If we don't have a converter stored we must ask the retrieved one through virtual calls.
        => preferredRepresentation switch
        {
            DataRepresentation.Binary when (Converter is not null ? _canBinaryConvert : converter.CanConvert(DataRepresentation.Binary))
                => DataRepresentation.Binary,
            DataRepresentation.Text when (Converter is not null ? !_canTextConvert : !converter.CanConvert(DataRepresentation.Text))
                => DataRepresentation.Binary,
            _ => (Converter is not null ? _canBinaryConvert : converter.CanConvert(DataRepresentation.Binary)) ? DataRepresentation.Binary : DataRepresentation.Text
        };

    void ThrowNotSupported() => throw new NotSupportedException();
}

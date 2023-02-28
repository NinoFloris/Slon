using System;
using System.Runtime.CompilerServices;
using Slon.Pg.Types;

namespace Slon.Pg;

abstract class PgConverterInfo
{
    readonly bool _canConvert;
    readonly bool _canTextConvert;
    readonly bool _isTypeDbNullable;

    protected PgConverterInfo(Type type, PgConverterOptions options)
    {
        Type = type;
        Options = options;
    }

    protected PgConverterInfo(Type type, PgConverterOptions options, PgConverter converter, PgTypeId pgTypeId)
    {
        Type = type;
        Options = options;
        Converter = converter;
        PgTypeId = options.GetCanonicalTypeId(pgTypeId);
        _canConvert = converter.CanConvert;
        _canTextConvert = converter.CanTextConvert;
        _isTypeDbNullable = converter.IsDbNullable;
    }

    public Type Type { get; }
    public PgConverterOptions Options { get; }

    // Whether this ConverterInfo maps to the default CLR Type for the DataTypeName given to IPgConverterInfoResolver.GetConverterInfo.
    public required bool IsDefault { get; init; }
    public DataRepresentation? PreferredRepresentation { get; init; }

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

    public PgTypeId GetPgTypeId<T>(T? value)
    {
        if (PgTypeId is { } pgTypeId)
            return pgTypeId;

        if (this is PgConverterResolverInfo<T> resolverInfoT)
            return Options.GetCanonicalTypeId(resolverInfoT.ConverterResolver.GetDataTypeName(value));

        ThrowNotSupported();
        return default;
    }

    public PgTypeId GetPgTypeIdAsObject(object? value)
    {
        if (PgTypeId is { } pgTypeId)
            return pgTypeId;

        if (this is PgConverterResolverInfo resolverInfo)
            return Options.GetCanonicalTypeId(resolverInfo.ConverterResolver.GetDataTypeNameAsObject(value));

        ThrowNotSupported();
        return default;
    }

    public PgConverter<T> GetConverter<T>(T? value)
    {
        if (this is PgConverterInfo<T> converterInfoT)
            return converterInfoT.Converter;

        if (this is PgConverterResolverInfo<T> resolverInfoT)
            return resolverInfoT.ConverterResolver.GetConverterInternal(value);

        ThrowNotSupported();
        return null!;
    }

    public PgConverter GetConverterAsObject(object? value)
    {
        if (Converter is not null)
            return Converter;

        if (this is PgConverterResolverInfo resolverInfo)
            return resolverInfo.ConverterResolver.GetConverterAsObjectInternal(value);

        ThrowNotSupported();
        return null!;
    }

    public SizeResult GetAnySize<T>(T? value, int bufferLength, out object? writeState, out DataRepresentation representation, DataRepresentation? preferredRepresentation = null)
    {
        writeState = null;
        var converter = GetConverter(value);
        preferredRepresentation ??= PreferredRepresentation;

        // If we don't have a static converter we must ask the retrieved one through virtual calls.
        representation = preferredRepresentation switch
        {
            DataRepresentation.Binary when (Converter is not null ? _canConvert : converter.CanConvert) => DataRepresentation.Binary,
            DataRepresentation.Text when (Converter is not null ? !_canTextConvert : !converter.CanTextConvert) => DataRepresentation.Binary,
            _ => (Converter is not null ? _canConvert : converter.CanConvert) ? DataRepresentation.Binary : DataRepresentation.Text
        };

        return representation switch
        {
            DataRepresentation.Binary => converter.GetSize(value!, bufferLength, ref writeState, Options),
            DataRepresentation.Text => converter.GetTextSize(value!, bufferLength, ref writeState, Options),
            _ => throw new ArgumentOutOfRangeException(nameof(representation), representation, null)
        };
    }

    public SizeResult GetAnySizeAsObject(object? value, int bufferLength, out object? writeState, out DataRepresentation representation, DataRepresentation? preferredRepresentation = null)
    {
        writeState = null;
        var converter = GetConverterAsObject(value);
        preferredRepresentation ??= PreferredRepresentation;

        // If we don't have a static converter we must ask the retrieved one through virtual calls.
        representation = preferredRepresentation switch
        {
            DataRepresentation.Binary when (Converter is not null ? _canConvert : converter.CanConvert) => DataRepresentation.Binary,
            DataRepresentation.Text when (Converter is not null ? !_canTextConvert : !converter.CanTextConvert) => DataRepresentation.Binary,
            _ => (Converter is not null ? _canConvert : converter.CanConvert) ? DataRepresentation.Binary : DataRepresentation.Text
        };

        return representation switch
        {
            DataRepresentation.Binary => converter.GetSizeAsObject(value!, bufferLength, ref writeState, Options),
            DataRepresentation.Text => converter.GetTextSizeAsObject(value!, bufferLength, ref writeState, Options),
            _ => throw new ArgumentOutOfRangeException(nameof(representation), representation, null)
        };
    }

    void ThrowNotSupported() => throw new NotSupportedException();

    public bool IsDbNullValue<T>(T? value)
    {
        if (Converter is not null && !_isTypeDbNullable)
            return false;

        var converter = GetConverter(value);
        return converter.IsDbNullValue(value, Options);
    }

    public bool IsDbNullValueAsObject(object? value)
    {
        if (Converter is not null && !_isTypeDbNullable)
            return false;

        var converter = GetConverterAsObject(value);
        return converter.IsDbNullValueAsObject(value, Options);
    }
}

sealed class PgConverterInfo<T> : PgConverterInfo
{
    public PgConverterInfo(PgConverterOptions options, PgConverter<T> converter, PgTypeId pgTypeId)
        : base(typeof(T), options, converter, pgTypeId)
    {
        DebugShim.Assert(Converter is not null);
    }

    public new PgConverter<T> Converter => Unsafe.As<PgConverter, PgConverter<T>>(ref Unsafe.AsRef(base.Converter!));
}

class PgConverterResolverInfo : PgConverterInfo
{
    internal PgConverterResolverInfo(PgConverterOptions options, PgConverterResolver converterResolver)
        : base(converterResolver.Type, options)
    {
        ConverterResolver = converterResolver;
    }

    public PgConverterResolver ConverterResolver { get; }
}

sealed class PgConverterResolverInfo<T>: PgConverterResolverInfo
{
    public PgConverterResolverInfo(PgConverterOptions options, PgConverterResolver<T> converterResolver) : base(options, converterResolver)
    {
    }

    public new PgConverterResolver<T> ConverterResolver => Unsafe.As<PgConverterResolver, PgConverterResolver<T>>(ref Unsafe.AsRef(base.ConverterResolver));
}

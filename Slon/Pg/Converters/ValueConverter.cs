using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Slon.Protocol;

namespace Slon.Pg.Converters;

// See https://github.com/dotnet/runtime/issues/80564 for an explanation of the existentce of the TConverter type parameter.
// It helps devirtualization during inlining in derived types (though not to the fullest extent yet).

/// A composing converter that allows for custom value conversions, it delegates all remaining behavior to the effective converter.
abstract class ValueConverter<T, TEffective, TConverter>: PgConverter<T> where TConverter : PgConverter<TEffective>
{
    readonly TConverter _effectiveConverter;
    protected ValueConverter(TConverter effectiveConverter)
        : base(FromDelegatedDbNullPredicate(effectiveConverter.DbNullPredicate, typeof(T)))
        => _effectiveConverter = effectiveConverter;

    protected TConverter EffectiveConverter => _effectiveConverter;
    protected abstract T ConvertFrom(TEffective value, PgConverterOptions options);
    protected abstract TEffective ConvertTo(T value, PgConverterOptions options);

    protected sealed override bool IsDbNull(T? value, PgConverterOptions options)
    {
        DebugShim.Assert(value is not null);
        return _effectiveConverter.IsDbNullValue(ConvertTo(value, options), options);
    }

    public sealed override bool CanConvert(DataRepresentation representation) => _effectiveConverter.CanConvert(representation);

    public sealed override SizeResult GetSize(T value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options)
        => _effectiveConverter.GetSize(ConvertTo(value, options), bufferLength, ref writeState, representation, options);

    // NOTE: Not sealed as reads often need some implementation adjustment beyond a simple conversion to be optimally efficient.
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out T value, PgConverterOptions options)
    {
        var status = _effectiveConverter.Read(ref reader, byteCount, out var effectiveValue, options);
        value = status is ReadStatus.Done ? ConvertFrom(effectiveValue, options) : default!;
        return status;
    }

    public sealed override void Write(PgWriter writer, T value, PgConverterOptions options)
        => _effectiveConverter.Write(writer, ConvertTo(value, options), options);

    public sealed override ValueTask WriteAsync(PgWriter writer, T value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => _effectiveConverter.WriteAsync(writer, ConvertTo(value, options), options, cancellationToken);
}

sealed class LambdaValueConverter<T, TEffective, TConverter> : ValueConverter<T, TEffective, TConverter> where TConverter : PgConverter<TEffective>
{
    readonly Func<TEffective, PgConverterOptions, T> _from;
    readonly Func<T, PgConverterOptions, TEffective> _to;

    public LambdaValueConverter(Func<TEffective, PgConverterOptions, T> from, Func<T, PgConverterOptions, TEffective> to, TConverter effectiveConverter) : base(effectiveConverter)
    {
        _from = from;
        _to = to;
    }

    protected override T ConvertFrom(TEffective value, PgConverterOptions options) => _from(value, options);
    protected override TEffective ConvertTo(T value, PgConverterOptions options) => _to(value, options);
}

using System;
using System.Buffers;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using Slon.Protocol;

namespace Slon.Pg.Converters;

/// A composing converter that converts number types, it delegates all behavior to the underlying converter.
sealed class NumberValueConverter<T, TEffective, TConverter> : PgConverter<T> where TConverter : PgConverter<TEffective>
#if !NETSTANDARD2_0
    where T : INumberBase<T> where TEffective : INumberBase<TEffective>
#endif
{
    readonly TConverter _effectiveConverter;
    public NumberValueConverter(TConverter effectiveConverter) => _effectiveConverter = effectiveConverter;

    TConverter EffectiveConverter => _effectiveConverter;

#if !NETSTANDARD2_0
    T ConvertFrom(TEffective value, PgConverterOptions options) => T.CreateChecked(value);
    TEffective ConvertTo(T value, PgConverterOptions options) => TEffective.CreateChecked(value);
#else
// TODO
    T ConvertFrom(TEffective value, PgConverterOptions options) => throw new NotImplementedException();
    TEffective ConvertTo(T value, PgConverterOptions options) => throw new NotImplementedException();
#endif

    public override bool CanConvert => _effectiveConverter.CanConvert;

    public override SizeResult GetSize(T value, int bufferLength, ref object? writeState, PgConverterOptions options)
        => _effectiveConverter.GetSize(ConvertTo(value, options), bufferLength, ref writeState, options);

    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out T value, PgConverterOptions options)
    {
        var status = _effectiveConverter.Read(ref reader, byteCount, out var effectiveValue, options);
        value = status is ReadStatus.Done ? ConvertFrom(effectiveValue, options) : default!;
        return status;
    }

    public override void Write(PgWriter writer, T value, PgConverterOptions options)
        => _effectiveConverter.Write(writer, ConvertTo(value, options), options);

    public override ValueTask WriteAsync(PgWriter writer, T value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => _effectiveConverter.WriteAsync(writer, ConvertTo(value, options), options, cancellationToken);

    public override bool CanTextConvert => _effectiveConverter.CanTextConvert;

    public override SizeResult GetTextSize(T value, int bufferLength, ref object? writeState, PgConverterOptions options)
        => _effectiveConverter.GetTextSize(ConvertTo(value, options), bufferLength, ref writeState, options);

    public override ReadStatus ReadText(ref SequenceReader<byte> reader, int byteCount, out T value, PgConverterOptions options)
    {
        var status = _effectiveConverter.ReadText(ref reader, byteCount, out var effectiveValue, options);
        value = status is ReadStatus.Done ? ConvertFrom(effectiveValue, options) : default!;
        return status;
    }

    public override void WriteText(PgWriter writer, T value, PgConverterOptions options)
        => _effectiveConverter.WriteText(writer, ConvertTo(value, options), options);

    public override ValueTask WriteTextAsync(PgWriter writer, T value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => _effectiveConverter.WriteTextAsync(writer, ConvertTo(value, options), options, cancellationToken);
}

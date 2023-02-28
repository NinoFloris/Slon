using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Slon.Protocol;

namespace Slon.Pg.Converters;

/// A composing converter that allows for custom value conversions, it delegates all remaining behavior to the underlying converter.
abstract class ValueConverter<T, TEffective, TConverter>: PgConverter<T> where TConverter : PgConverter<TEffective>
{
    readonly TConverter _effectiveConverter;
    protected ValueConverter(TConverter effectiveConverter) => _effectiveConverter = effectiveConverter;

    protected TConverter EffectiveConverter => _effectiveConverter;
    protected abstract T ConvertFrom(TEffective value, PgConverterOptions options);
    protected abstract TEffective ConvertTo(T value, PgConverterOptions options);

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

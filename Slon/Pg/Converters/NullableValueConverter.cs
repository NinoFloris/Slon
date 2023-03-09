using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Slon.Protocol;

namespace Slon.Pg.Converters;

// NULL writing is always responsibility of the caller writing the length, so there is not much we do here.
// NOTE: We don't inherit from ValueConverter to be able to avoid the virtual calls for ConvertFrom/ConvertTo.

/// Special value converter to be able to use struct converters as System.Nullable converters, it delegates all behavior to the effective converter.
sealed class NullableValueConverter<T> : PgConverter<T?> where T : struct
{
    readonly PgConverter<T> _effectiveConverter;
    public NullableValueConverter(PgConverter<T> effectiveConverter)
        : base(FromDelegatedDbNullPredicate(effectiveConverter.DbNullPredicate, typeof(T)))
        => _effectiveConverter = effectiveConverter;

    T? ConvertFrom(T value, PgConverterOptions options) => value;
    T ConvertTo(T? value, PgConverterOptions options) => value.GetValueOrDefault();

    protected override bool IsDbNull(T? value, PgConverterOptions options)
        => _effectiveConverter.IsDbNullValue(ConvertTo(value, options), options);

    public override bool CanConvert(DataRepresentation representation) => _effectiveConverter.CanConvert(representation);

    public override SizeResult GetSize(T? value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options)
        => _effectiveConverter.GetSize(ConvertTo(value, options), bufferLength, ref writeState, representation, options);

    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out T? value, PgConverterOptions options)
    {
        var status = _effectiveConverter.Read(ref reader, byteCount, out var effectiveValue, options);
        value = status is ReadStatus.Done ? ConvertFrom(effectiveValue, options) : default!;
        return status;
    }

    public override void Write(PgWriter writer, T? value, PgConverterOptions options)
        => _effectiveConverter.Write(writer, ConvertTo(value, options), options);

    public override ValueTask WriteAsync(PgWriter writer, T? value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => _effectiveConverter.WriteAsync(writer, ConvertTo(value, options), options, cancellationToken);
}

using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg.Converters;

// NULL writing is always responsibility of the caller writing the length, so there is not much we do here.
// NOTE: We don't inherit from ValueConverter to be able to avoid the virtual calls for ConvertFrom/ConvertTo.

/// Special value converter to be able to use struct converters as System.Nullable converters, it delegates all behavior to the effective converter.
sealed class NullableValueConverter<T> : PgBufferedConverter<T?> where T : struct
{
    readonly PgBufferedConverter<T> _effectiveConverter;
    public NullableValueConverter(PgBufferedConverter<T> effectiveConverter)
        : base(effectiveConverter.DbNullPredicateKind is DbNullPredicate.Extended)
        => _effectiveConverter = effectiveConverter;

    T? ConvertFrom(T value) => value;
    T ConvertTo(T? value) => value.GetValueOrDefault();

    protected override bool IsDbNull(T? value)
        => _effectiveConverter.IsDbNullValue(ConvertTo(value));

    public override bool CanConvert(DataFormat format, out bool fixedSize) => _effectiveConverter.CanConvert(format, out fixedSize);

    public override ValueSize GetSize(ref SizeContext context, [DisallowNull]T? value)
        => _effectiveConverter.GetSize(ref context, ConvertTo(value));

    protected override T? ReadCore(PgReader reader) => ConvertFrom(_effectiveConverter.Read(reader));

    public override void Write(PgWriter writer, T? value)
        => _effectiveConverter.Write(writer, ConvertTo(value));
}

/// Special value converter to be able to use struct converters as System.Nullable converters, it delegates all behavior to the effective converter.
sealed class StreamingNullableValueConverter<T> : PgStreamingConverter<T?> where T : struct
{
    readonly PgStreamingConverter<T> _effectiveConverter;
    public StreamingNullableValueConverter(PgStreamingConverter<T> effectiveConverter)
        : base(effectiveConverter.DbNullPredicateKind is DbNullPredicate.Extended)
        => _effectiveConverter = effectiveConverter;

    T ConvertTo(T? value) => value.GetValueOrDefault();

    protected override bool IsDbNull(T? value)
        => _effectiveConverter.IsDbNullValue(ConvertTo(value));

    public override bool CanConvert(DataFormat format, out bool fixedSize) => _effectiveConverter.CanConvert(format, out fixedSize);

    public override T? Read(PgReader reader) => _effectiveConverter.Read(reader);

    public override ValueSize GetSize(ref SizeContext context, [DisallowNull]T? value)
        => _effectiveConverter.GetSize(ref context, ConvertTo(value));

    public override void Write(PgWriter writer, T? value)
        => _effectiveConverter.Write(writer, ConvertTo(value));

    public override async ValueTask<T?> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => await _effectiveConverter.ReadAsync(reader, cancellationToken);

    public override ValueTask WriteAsync(PgWriter writer, T? value, CancellationToken cancellationToken = default)
        => _effectiveConverter.WriteAsync(writer, ConvertTo(value), cancellationToken);
}

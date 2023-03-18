using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg.Converters;

// NULL writing is always responsibility of the caller writing the length, so there is not much we do here.
// NOTE: We don't inherit from ValueConverter to be able to avoid the virtual calls for ConvertFrom/ConvertTo.

/// Special value converter to be able to use struct converters as System.Nullable converters, it delegates all behavior to the effective converter.
sealed class NullableValueConverter<T> : PgConverter<T?> where T : struct
{
    readonly PgConverter<T> _effectiveConverter;
    public NullableValueConverter(PgConverter<T> effectiveConverter)
        : base(effectiveConverter.DbNullPredicateKind is DbNullPredicate.Extended)
        => _effectiveConverter = effectiveConverter;

    T? ConvertFrom(T value) => value;
    T ConvertTo(T? value) => value.GetValueOrDefault();

    protected override bool IsDbNull(T? value)
        => _effectiveConverter.IsDbNullValue(ConvertTo(value));

    public override bool CanConvert(DataFormat format) => _effectiveConverter.CanConvert(format);

    public override ValueSize GetSize(SizeContext context, T? value, ref object? writeState)
        => _effectiveConverter.GetSize(context, ConvertTo(value), ref writeState);

    public override T? Read(PgReader reader)
        => ConvertFrom(_effectiveConverter.Read(reader));

    public override void Write(PgWriter writer, T? value)
        => _effectiveConverter.Write(writer, ConvertTo(value));
}

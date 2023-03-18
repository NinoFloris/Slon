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
        : base(FromDelegatedDbNullPredicate(effectiveConverter.DbNullPredicate, typeof(T)))
        => _effectiveConverter = effectiveConverter;

    T? ConvertFrom(T value, PgConverterOptions options) => value;
    T ConvertTo(T? value, PgConverterOptions options) => value.GetValueOrDefault();

    protected override bool IsDbNull(T? value, PgConverterOptions options)
        => _effectiveConverter.IsDbNullValue(ConvertTo(value, options), options);

    public override bool CanConvert(DataFormat format) => _effectiveConverter.CanConvert(format);

    public override ValueSize GetSize(T? value, ref object? writeState, SizeContext context, PgConverterOptions options)
        => _effectiveConverter.GetSize(ConvertTo(value, options), ref writeState, context, options);

    public override T? Read(PgReader reader, PgConverterOptions options)
        => ConvertFrom(_effectiveConverter.Read(reader, options), options);

    public override ValueTask<T?> ReadAsync(PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default)
    {
        var task = _effectiveConverter.ReadAsync(reader, options, cancellationToken);
        return task.IsCompletedSuccessfully ? new(ConvertFrom(task.GetAwaiter().GetResult(), options)) : Core(task);

        async ValueTask<T?> Core(ValueTask<T> task) => ConvertFrom(await task, options);
    }

    public override void Write(PgWriter writer, T? value, PgConverterOptions options)
        => _effectiveConverter.Write(writer, ConvertTo(value, options), options);

    public override ValueTask WriteAsync(PgWriter writer, T? value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => _effectiveConverter.WriteAsync(writer, ConvertTo(value, options), options, cancellationToken);
}

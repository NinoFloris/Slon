using System;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg.Converters;

/// A composing converter that allows for custom value conversions, it delegates all remaining behavior to the effective converter.
abstract class ValueConverter<T, TEffective>: PgConverter<T>
{
    readonly PgConverter<TEffective> _effectiveConverter;
    protected ValueConverter(PgConverter<TEffective> effectiveConverter)
        : base(FromDelegatedDbNullPredicate(effectiveConverter.DbNullPredicate, typeof(T)))
        => _effectiveConverter = effectiveConverter;

    protected PgConverter<TEffective> EffectiveConverter => _effectiveConverter;
    protected abstract T ConvertFrom(TEffective value, PgConverterOptions options);
    protected abstract TEffective ConvertTo(T value, PgConverterOptions options);

    protected sealed override bool IsDbNull(T? value, PgConverterOptions options)
    {
        DebugShim.Assert(value is not null);
        return _effectiveConverter.IsDbNullValue(ConvertTo(value, options), options);
    }

    public sealed override bool CanConvert(DataRepresentation representation) => _effectiveConverter.CanConvert(representation);

    public sealed override ValueSize GetSize(T value, ref object? writeState, SizeContext context, PgConverterOptions options)
        => _effectiveConverter.GetSize(ConvertTo(value, options), ref writeState, context, options);

    // NOTE: Not sealed as reads often need some implementation adjustment beyond a simple conversion to be optimally efficient.
    public override T Read(PgReader reader, PgConverterOptions options)
        => ConvertFrom(_effectiveConverter.Read(reader, options), options);

    // NOTE: Not sealed as reads often need some implementation adjustment beyond a simple conversion to be optimally efficient.
    public override ValueTask<T> ReadAsync(PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default)
    {
        var task = _effectiveConverter.ReadAsync(reader, options, cancellationToken);
        return task.IsCompletedSuccessfully ? new(ConvertFrom(task.GetAwaiter().GetResult(), options)) : Core(task);

        async ValueTask<T> Core(ValueTask<TEffective> task) => ConvertFrom(await task, options);
    }

    public sealed override void Write(PgWriter writer, T value, PgConverterOptions options)
        => _effectiveConverter.Write(writer, ConvertTo(value, options), options);

    public sealed override ValueTask WriteAsync(PgWriter writer, T value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => _effectiveConverter.WriteAsync(writer, ConvertTo(value, options), options, cancellationToken);
}

sealed class LambdaValueConverter<T, TEffective> : ValueConverter<T, TEffective>
{
    readonly Func<TEffective, PgConverterOptions, T> _from;
    readonly Func<T, PgConverterOptions, TEffective> _to;

    public LambdaValueConverter(Func<TEffective, PgConverterOptions, T> from, Func<T, PgConverterOptions, TEffective> to, PgConverter<TEffective> effectiveConverter) : base(effectiveConverter)
    {
        _from = from;
        _to = to;
    }

    protected override T ConvertFrom(TEffective value, PgConverterOptions options) => _from(value, options);
    protected override TEffective ConvertTo(T value, PgConverterOptions options) => _to(value, options);
}

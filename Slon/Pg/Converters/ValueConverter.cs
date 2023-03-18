using System;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg.Converters;

/// A composing converter that allows for custom value conversions, it delegates all remaining behavior to the effective converter.
abstract class ValueConverter<T, TEffective>: PgConverter<T>
{
    readonly PgConverter<TEffective> _effectiveConverter;
    protected ValueConverter(PgConverter<TEffective> effectiveConverter)
        : base(effectiveConverter.DbNullPredicateKind is DbNullPredicate.Extended)
        => _effectiveConverter = effectiveConverter;

    protected PgConverter<TEffective> EffectiveConverter => _effectiveConverter;
    protected abstract T? ConvertFrom(TEffective? value);
    protected abstract TEffective ConvertTo(T value);

    protected sealed override bool IsDbNull(T? value)
    {
        DebugShim.Assert(value is not null);
        return _effectiveConverter.IsDbNullValue(ConvertTo(value));
    }

    public sealed override bool CanConvert(DataFormat format) => _effectiveConverter.CanConvert(format);

    public sealed override ValueSize GetSize(SizeContext context, T value, ref object? writeState)
        => _effectiveConverter.GetSize(context, ConvertTo(value), ref writeState);

    // NOTE: Not sealed as reads often need some implementation adjustment beyond a simple conversion to be optimally efficient.
    public override T? Read(PgReader reader)
        => ConvertFrom(_effectiveConverter.Read(reader));

    // NOTE: Not sealed as reads often need some implementation adjustment beyond a simple conversion to be optimally efficient.
    public override ValueTask<T?> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
    {
        var task = _effectiveConverter.ReadAsync(reader, cancellationToken);
        return task.IsCompletedSuccessfully ? new(ConvertFrom(task.GetAwaiter().GetResult())) : Core(task);

        async ValueTask<T?> Core(ValueTask<TEffective?> task) => ConvertFrom(await task);
    }

    public sealed override void Write(PgWriter writer, T value)
        => _effectiveConverter.Write(writer, ConvertTo(value));

    public sealed override ValueTask WriteAsync(PgWriter writer, T value, CancellationToken cancellationToken = default)
        => _effectiveConverter.WriteAsync(writer, ConvertTo(value), cancellationToken);
}

sealed class LambdaValueConverter<T, TEffective> : ValueConverter<T, TEffective>
{
    readonly Func<TEffective?, T?> _from;
    readonly Func<T, TEffective> _to;

    public LambdaValueConverter(Func<TEffective?, T?> from, Func<T, TEffective> to, PgConverter<TEffective> effectiveConverter) : base(effectiveConverter)
    {
        _from = from;
        _to = to;
    }

    protected override T? ConvertFrom(TEffective? value) => _from(value);
    protected override TEffective ConvertTo(T value) => _to(value);
}

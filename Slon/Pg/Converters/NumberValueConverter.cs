using System;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg.Converters;

// NOTE: We don't inherit from ValueConverter to be able to avoid the virtual calls for ConvertFrom/ConvertTo.

/// A value converter that converts number types, it delegates all behavior to the effective converter.
sealed class NumberValueConverter<T, TEffective> : PgConverter<T>
#if !NETSTANDARD2_0
    where T : INumberBase<T> where TEffective : INumberBase<TEffective>
#endif
{
    readonly PgConverter<TEffective> _effectiveConverter;
    public NumberValueConverter(PgConverter<TEffective> effectiveConverter)
        : base(FromDelegatedDbNullPredicate(effectiveConverter.DbNullPredicate, typeof(T)))
        => _effectiveConverter = effectiveConverter;

#if !NETSTANDARD2_0
    T ConvertFrom(TEffective value, PgConverterOptions options) => T.CreateChecked(value);
    TEffective ConvertTo(T value, PgConverterOptions options) => TEffective.CreateChecked(value);
#else
// TODO
    T ConvertFrom(TEffective value, PgConverterOptions options) => throw new NotImplementedException();
    TEffective ConvertTo(T value, PgConverterOptions options) => throw new NotImplementedException();
#endif

    protected override bool IsDbNull(T? value, PgConverterOptions options)
    {
        DebugShim.Assert(value is not null);
        return _effectiveConverter.IsDbNullValue(ConvertTo(value, options), options);
    }

    public override bool CanConvert(DataRepresentation representation) => _effectiveConverter.CanConvert(representation);

    public override ValueSize GetSize(T value, ref object? writeState, SizeContext context, PgConverterOptions options)
        => _effectiveConverter.GetSize(ConvertTo(value, options), ref writeState, context, options);

    public override T Read(PgReader reader, PgConverterOptions options)
        => ConvertFrom(_effectiveConverter.Read(reader, options), options);

    public override ValueTask<T> ReadAsync(PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default)
    {
        var task = _effectiveConverter.ReadAsync(reader, options, cancellationToken);
        return task.IsCompletedSuccessfully ? new(ConvertFrom(task.GetAwaiter().GetResult(), options)) : Core(task);

        async ValueTask<T> Core(ValueTask<TEffective> task) => ConvertFrom(await task, options);
    }

    public override void Write(PgWriter writer, T value, PgConverterOptions options)
        => _effectiveConverter.Write(writer, ConvertTo(value, options), options);

    public override ValueTask WriteAsync(PgWriter writer, T value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => _effectiveConverter.WriteAsync(writer, ConvertTo(value, options), options, cancellationToken);
}

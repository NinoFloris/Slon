using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg.Converters;

/// <summary>
/// A converter to map strongly typed apis onto boxed converter results to produce a strongly typed converter over T.
/// </summary>
sealed class CastingConverter<T> : PgStreamingConverter<T>
{
    readonly PgConverter _effectiveConverter;
    public CastingConverter(PgConverter effectiveConverter)
        : base(effectiveConverter.DbNullPredicateKind is DbNullPredicate.Extended)
        => _effectiveConverter = effectiveConverter;

    protected override bool IsDbNull(T? value)
    {
        Debug.Assert(value is not null);
        return _effectiveConverter.IsDbNullValueAsObject(value);
    }

    public override bool CanConvert(DataFormat format, out bool fixedSize) => _effectiveConverter.CanConvert(format, out fixedSize);

    public override ValueSize GetSize(ref SizeContext context, T value)
        => _effectiveConverter.GetSizeAsObject(ref context, value!);

    public override T Read(PgReader reader)
        => (T)_effectiveConverter.ReadAsObject(reader)!;

    public override ValueTask<T> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
    {
        var task = _effectiveConverter.ReadAsObjectAsync(reader, cancellationToken);
        return task.IsCompletedSuccessfully ? new((T)task.GetAwaiter().GetResult()!) : Core(task);

        async ValueTask<T> Core(ValueTask<object?> task) => (T)(await task)!;
    }

    public override void Write(PgWriter writer, T value)
        => _effectiveConverter.WriteAsObject(writer, value!);

    public override ValueTask WriteAsync(PgWriter writer, T value, CancellationToken cancellationToken = default)
        => _effectiveConverter.WriteAsObjectAsync(writer, value!, cancellationToken);
}

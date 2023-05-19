using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Converters;

namespace Slon.Pg;

struct SizeContext
{
    public SizeContext(DataFormat format, int bufferLength)
    {
        Format = format;
        BufferLength = bufferLength;
    }

    public DataFormat Format { get; }
    public int BufferLength { get; }
    public object? WriteState { get; set; }
}

abstract class PgConverter
{
    internal DbNullPredicate DbNullPredicateKind { get; }

    private protected PgConverter(bool extendedDbNullPredicate = false)
        // We rely on TypeToConvert to be able to keep this constructor code in the non generic base class, reducing code size.
        // ReSharper disable once VirtualMemberCallInConstructor
        => DbNullPredicateKind = GetDbNullPredicateForType(extendedDbNullPredicate ? DbNullPredicate.Extended : null, TypeToConvert);

    public bool IsDbNullable => DbNullPredicateKind is not DbNullPredicate.None;

    /// FixedSize is true when GetSize can be called with a default value for the type and the given format without throwing.
    public virtual bool CanConvert(DataFormat format, out bool fixedSize)
    {
        fixedSize = false;
        return format is DataFormat.Binary;
    }

    internal object? ReadAsObject(PgReader reader)
        => ReadAsObject(async: false, reader).GetAwaiter().GetResult();
    internal ValueTask<object?> ReadAsObjectAsync(PgReader reader, CancellationToken cancellationToken = default)
        => ReadAsObject(async: true, reader, cancellationToken);

    internal void WriteAsObject(PgWriter writer, object value)
        => WriteAsObject(async: false, writer, value).GetAwaiter().GetResult();
    internal ValueTask WriteAsObjectAsync(PgWriter writer, object value, CancellationToken cancellationToken = default)
        => WriteAsObject(async: true, writer, value, cancellationToken);

    internal abstract Type TypeToConvert { get; }
    internal abstract bool IsDbNullValueAsObject([NotNullWhen(false)]object? value);
    internal abstract ValueSize GetSizeAsObject(ref SizeContext context, object value);

    // Shared sync/async abstract to reduce virtual method table size overhead and code size for each NpgsqlConverter<T> instantiation.
    private protected abstract ValueTask<object?> ReadAsObject(bool async, PgReader reader, CancellationToken cancellationToken = default);
    // Shared sync/async abstract to reduce virtual method table size overhead and code size for each NpgsqlConverter<T> instantiation.
    private protected abstract ValueTask WriteAsObject(bool async, PgWriter writer, object value, CancellationToken cancellationToken = default);

    private protected virtual ValueTask<object?> BoxResult(Task task) => throw new NotSupportedException();

    // Split out from the generic class to amortize the huge size penalty per async state machine, which would otherwise be per instantiation.
#if !NETSTANDARD2_0
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
    private protected async ValueTask<object?> AwaitReadTask(Task task)
    {
        await task;
        return BoxResult(task);
    }

    static DbNullPredicate GetDbNullPredicateForType(DbNullPredicate? dbNullPredicate, Type type)
    {
        if (dbNullPredicate is { } value)
            return value;

        if (!type.IsValueType)
        {
            if (type == typeof(object) || type == typeof(DBNull))
                return DbNullPredicate.PolymorphicNull;
            return DbNullPredicate.Null;
        }

        if (type.GetGenericTypeDefinition() == typeof(Nullable<>))
            return DbNullPredicate.Null;

        return DbNullPredicate.None;
    }

    internal enum DbNullPredicate: byte
    {
        /// Never DbNull
        None,
        /// DbNull when value is null
        Null,
        /// DbNull when value is null or DBNull
        PolymorphicNull,
        /// DbNull when value is null or *user code*
        Extended
    }

    private protected static void ThrowIORequired() => throw new InvalidOperationException("Fixed sizedness for format not respected, expected no IO to be required.");
}

abstract class PgConverter<T> : PgConverter
{
    private protected PgConverter(bool extendedDbNullPredicate) : base(extendedDbNullPredicate) { }

    protected virtual bool IsDbNull(T? value) => throw new NotImplementedException();

    public bool IsDbNullValue([NotNullWhen(false)] T? value)
        => value is null || DbNullPredicateKind is DbNullPredicate.PolymorphicNull && value is DBNull || DbNullPredicateKind is DbNullPredicate.Extended && IsDbNull(value);

    public abstract T Read(PgReader reader);
    public abstract ValueSize GetSize(ref SizeContext context, [DisallowNull]T value);
    public abstract void Write(PgWriter writer, [DisallowNull]T value);

    internal sealed override Type TypeToConvert => typeof(T);

    // Object null semantics as follows, if T is a struct (so excluding nullable) report false for null values, don't throw on the cast.
    // As a result this creates symmetry with IsDbNullValue when we're dealing with a struct T, as it cannot be passed null at all.
    internal sealed override bool IsDbNullValueAsObject([NotNullWhen(false)]object? value)
        => (default(T) is null || value is not null) && IsDbNullValue((T?)value);

    internal sealed override ValueSize GetSizeAsObject(ref SizeContext context, object value)
        => GetSize(ref context, (T)value);

    private protected override ValueTask<object?> ReadAsObject(bool async, PgReader reader, CancellationToken cancellationToken = default)
        => new(Read(reader));

    private protected override ValueTask WriteAsObject(bool async, PgWriter writer, object value, CancellationToken cancellationToken = default)
    {
        Write(writer, (T)value);
        return new();
    }
}

abstract class PgStreamingConverter<T> : PgConverter<T>
{
    protected PgStreamingConverter(bool extendedDbNullPredicate = false) : base(extendedDbNullPredicate) { }
    public abstract ValueTask<T> ReadAsync(PgReader reader, CancellationToken cancellationToken = default);
    public abstract ValueTask WriteAsync(PgWriter writer, [DisallowNull]T value, CancellationToken cancellationToken = default);

    private protected sealed override ValueTask<object?> BoxResult(Task task) => new(new ValueTask<object?>((Task<T>)task).Result);
    private protected sealed override ValueTask<object?> ReadAsObject(bool async, PgReader reader, CancellationToken cancellationToken = default)
    {
        if (!async)
            return new(Read(reader));

        var task = ReadAsync(reader, cancellationToken);
        if (task.IsCompletedSuccessfully)
            return new(task.Result);

        return AwaitReadTask(task.AsTask());
    }

    private protected sealed override ValueTask WriteAsObject(bool async, PgWriter writer, object value, CancellationToken cancellationToken = default)
    {
        if (async)
            return WriteAsync(writer, (T)value, cancellationToken);

        Write(writer, (T)value);
        return new();
    }
}

static class PgConverterOfTExtensions
{
    public static ValueTask<T> ReadAsync<T>(this PgConverter<T> converter, PgReader reader, CancellationToken cancellationToken)
    {
        if (converter is PgStreamingConverter<T> asyncConverter)
            return asyncConverter.ReadAsync(reader, cancellationToken);

        return new(converter.Read(reader));
    }

    public static ValueTask WriteAsync<T>(this PgConverter<T> converter, PgWriter writer, [DisallowNull]T value, CancellationToken cancellationToken)
    {
        if (converter is PgStreamingConverter<T> asyncConverter)
            return asyncConverter.WriteAsync(writer, value, cancellationToken);

        converter.Write(writer, value);
        return new();
    }
}

abstract class PgBufferedConverter<T> : PgConverter<T>
{
    protected PgBufferedConverter(bool extendedDbNullPredicate = false) : base(extendedDbNullPredicate) { }

    public override bool CanConvert(DataFormat format, out bool fixedSize) => fixedSize = format is DataFormat.Binary;

    protected abstract T ReadCore(PgReader reader);
    public sealed override T Read(PgReader reader)
    {
        if (reader.Remaining < reader.ByteCount)
            ThrowIORequired();

        return ReadCore(reader);
    }

    // In Npgsql we need this part too.
    // protected abstract void WriteCore(PgWriter writer, T value);
    // public override void Write(PgWriter writer, T value)
    // {
    //     var state = (object?)null;
    //     if (writer.Remaining < GetSize(default, value, ref state).Value)
    //         ThrowIORequired();
    //
    //     WriteCore(writer, value);
    // }
}

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
        DebugShim.Assert(value is not null);
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


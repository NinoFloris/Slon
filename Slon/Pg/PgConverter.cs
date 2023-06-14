using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

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

    /// When <see cref="fixedSize"/> is true GetSize can be called with a default value for the type and the given format without throwing.
    public virtual bool CanConvert(DataFormat format, out bool fixedSize)
    {
        fixedSize = false;
        return format is DataFormat.Binary;
    }

    internal object? ReadAsObject(PgReader reader)
        => ReadAsObject(async: false, reader).GetAwaiter().GetResult();
    internal ValueTask<object?> ReadAsObjectAsync(PgReader reader, CancellationToken cancellationToken = default)
        => ReadAsObject(async: true, reader, cancellationToken);

    /// When <see cref="value"/> is null the default value for the type is used.
    internal void WriteAsObject(PgWriter writer, object? value)
        => WriteAsObject(async: false, writer, value).GetAwaiter().GetResult();

    /// When <see cref="value"/> is null the default value for the type is used.
    internal ValueTask WriteAsObjectAsync(PgWriter writer, object? value, CancellationToken cancellationToken = default)
        => WriteAsObject(async: true, writer, value, cancellationToken);

    internal abstract Type TypeToConvert { get; }
    internal abstract bool IsDbNullValueAsObject([NotNullWhen(false)]object? value);
    internal abstract ValueSize GetSizeAsObject(ref SizeContext context, object? value);

    // Shared sync/async abstract to reduce virtual method table size overhead and code size for each NpgsqlConverter<T> instantiation.
    private protected abstract ValueTask<object?> ReadAsObject(bool async, PgReader reader, CancellationToken cancellationToken = default);
    // Shared sync/async abstract to reduce virtual method table size overhead and code size for each NpgsqlConverter<T> instantiation.
    private protected abstract ValueTask WriteAsObject(bool async, PgWriter writer, object? value, CancellationToken cancellationToken = default);

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

    [DoesNotReturn]
    private protected static void ThrowIORequired() => throw new InvalidOperationException("Fixed sizedness for format not respected, expected no IO to be required.");
    [DoesNotReturn]
    private protected static void ThrowNullReferenceException() => throw new NullReferenceException("Cannot write null value for reference type.");
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

    internal sealed override ValueSize GetSizeAsObject(ref SizeContext context, object? value)
        => GetSize(ref context, GetValueOrNRE(value));

    private protected override ValueTask<object?> ReadAsObject(bool async, PgReader reader, CancellationToken cancellationToken = default)
        => new(Read(reader));

    // When writing null for a struct type we want to pass default instead of failing the cast with a NullReferenceException.
    // This allows us to do polymorphic writing of default values i.e. without having to know T.
    // For reference types passing a null value is invalid as IsDbNullValue will always return true.
    private protected override ValueTask WriteAsObject(bool async, PgWriter writer, object? value, CancellationToken cancellationToken = default)
    {
        Write(writer, GetValueOrNRE(value));
        return new();
    }

    [return: NotNull]
    private protected static T GetValueOrNRE(object? value)
    {
        if (default(T) is not null)
            return (T)(value ?? default)!;

        if (value is null)
            ThrowNullReferenceException();

        return (T)value;
    }
}

// Using a function pointer here is safe against assembly unloading as the instance reference that the static pointer method lives on is passed
// as such the instance cannot be collected by the gc which means the entire assembly is prevented from unloading until we're done.
// The alternatives are:
// 1. Add a virtual method to PgConverter and make AwaitReadTask call into it (bloating the vtable of all PgConverter derived types).
// 2. Using a delegate (a static field + an alloc per T + metadata, slightly slower dispatch perf, so strictly worse as well).
static class PgStreamingConverter
{
    // Split out from the generic class to amortize the huge size penalty per async state machine, which would otherwise be per instantiation.
#if !NETSTANDARD
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
    public static async ValueTask<object?> AwaitTask(Task task, Continuation continuation)
    {
        await task;
        var result = continuation.Invoke(task);
        // Guarantee the type stays loaded until the function pointer call is done.
        GC.KeepAlive(continuation.Handle);
        return result;
    }

    // Split out into a struct as unsafe and async don't mix, while we do want a nicely typed function pointer signature to prevent mistakes.
    public readonly unsafe struct Continuation
    {
        public object Handle { get; }
        readonly delegate*<Task, ValueTask<object?>> _continuation;

        /// <param name="handle">A reference to the type that houses the static method <see cref="continuation"/> points to.</param>
        /// <param name="continuation">The continuation</param>
        public Continuation(object handle, delegate*<Task, ValueTask<object?>> continuation)
        {
            Handle = handle;
            _continuation = continuation;
        }

        public ValueTask<object?> Invoke(Task task) => _continuation(task);
    }
}

abstract class PgStreamingConverter<T> : PgConverter<T>
{
    protected PgStreamingConverter(bool extendedDbNullPredicate = false) : base(extendedDbNullPredicate) { }

    public abstract ValueTask<T> ReadAsync(PgReader reader, CancellationToken cancellationToken = default);
    public abstract ValueTask WriteAsync(PgWriter writer, [DisallowNull]T value, CancellationToken cancellationToken = default);

    private protected sealed override unsafe ValueTask<object?> ReadAsObject(bool async, PgReader reader, CancellationToken cancellationToken = default)
    {
        if (!async)
            return new(Read(reader));

        if (ReadAsync(reader, cancellationToken) is { IsCompletedSuccessfully: true } task)
            return new(task.Result);

        return PgStreamingConverter.AwaitTask(Task.CompletedTask, new(this, &BoxResult));

        // Using .Result on ValueTask is equivalent to GetAwaiter().GetResult(), this removes TaskAwaiter<TElement> rooting.
        static ValueTask<object?> BoxResult(Task task) => new(new ValueTask<object?>((Task<T>)task).Result);
    }

    // When writing null for a struct type we want to pass default instead of failing the cast with a NullReferenceException.
    // This allows us to do polymorphic writing of default values i.e. without having to know T.
    private protected sealed override ValueTask WriteAsObject(bool async, PgWriter writer, object? value, CancellationToken cancellationToken = default)
    {
        if (async)
            return WriteAsync(writer, GetValueOrNRE(value), cancellationToken);

        Write(writer, GetValueOrNRE(value));
        return new();
    }
}

static class PgConverterOfTExtensions
{
    public static ValueTask<T> ReadAsync<T>(this PgConverter<T> converter, PgReader reader, CancellationToken cancellationToken = default)
    {
        if (converter is PgStreamingConverter<T> asyncConverter)
            return asyncConverter.ReadAsync(reader, cancellationToken);

        return new(converter.Read(reader));
    }

    public static ValueTask WriteAsync<T>(this PgConverter<T> converter, PgWriter writer, [DisallowNull]T value, CancellationToken cancellationToken = default)
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
}

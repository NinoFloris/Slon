using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg;

readonly struct SizeContext
{
    public SizeContext(DataFormat format, int bufferLength)
    {
        Format = format;
        BufferLength = bufferLength;
    }

    public DataFormat Format { get; }
    public int BufferLength { get; }
}

abstract class PgConverter
{
    internal DbNullPredicate DbNullPredicateKind { get; }

    private protected PgConverter(bool extendedDbNullPredicate = false)
        // We rely on TypeToConvert to be able to keep this constructor code in the non generic base class, reducing code size.
        // ReSharper disable once VirtualMemberCallInConstructor
        => DbNullPredicateKind = GetDbNullPredicateForType(extendedDbNullPredicate ? DbNullPredicate.Extended : null, TypeToConvert);

    public bool IsDbNullable => DbNullPredicateKind is not DbNullPredicate.None;
    public virtual bool CanConvert(DataFormat format) => format is DataFormat.Binary;

    /// This method returns true when GetSize can be called with a default value for the type and the given representation without throwing.
    public virtual bool HasFixedSize(DataFormat format) => false;

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
    internal abstract ValueSize GetSizeAsObject(SizeContext context, object value, [NotNullIfNotNull(nameof(writeState))]ref object? writeState);

    // Shared sync/async abstract to reduce virtual method table size overhead and code size for each NpgsqlConverter<T> instantiation.
    private protected abstract ValueTask<object?> ReadAsObject(bool async, PgReader reader, CancellationToken cancellationToken = default);
    // Shared sync/async abstract to reduce virtual method table size overhead and code size for each NpgsqlConverter<T> instantiation.
    private protected abstract ValueTask WriteAsObject(bool async, PgWriter writer, object value, CancellationToken cancellationToken = default);

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
}

abstract class PgConverter<T> : PgConverter
{
    protected PgConverter(bool extendedDbNullPredicate = false) : base(extendedDbNullPredicate) { }

    protected virtual bool IsDbNull(T? value)
        => DbNullPredicateKind is DbNullPredicate.PolymorphicNull ? value is DBNull : throw new NotImplementedException();

    public bool IsDbNullValue([NotNullWhen(false)] T? value)
        => value is null || (DbNullPredicateKind is not (DbNullPredicate.None or DbNullPredicate.Null) && IsDbNull(value));

    public abstract T? Read(PgReader reader);

    public virtual ValueTask<T?> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => new(Read(reader));

    public abstract ValueSize GetSize(SizeContext context, T value, [NotNullIfNotNull(nameof(writeState))]ref object? writeState);
    public abstract void Write(PgWriter writer, T value);

    public virtual ValueTask WriteAsync(PgWriter writer, T value, CancellationToken cancellationToken = default)
    {
        Write(writer, value);
        return new();
    }

    internal sealed override Type TypeToConvert => typeof(T);

    // Object null semantics as follows, if T is a struct (so excluding nullable) report false for null values, don't throw on the cast.
    // As a result this creates symmetry with IsDbNullValue when we're dealing with a struct T, as it cannot be passed null at all.
    internal sealed override bool IsDbNullValueAsObject([NotNullWhen(false)]object? value)
        => (default(T) is null || value is not null) && IsDbNullValue((T?)value);

    internal sealed override ValueSize GetSizeAsObject(SizeContext context, object value, [NotNullIfNotNull(nameof(writeState))]ref object? writeState)
        => GetSize(context, (T)value, ref writeState);

    private protected sealed override ValueTask<object?> ReadAsObject(bool async, PgReader reader, CancellationToken cancellationToken = default)
    {
        return async ? ReadAsyncCast(reader, cancellationToken) : new(Read(reader));

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        async ValueTask<object?> ReadAsyncCast(PgReader reader, CancellationToken cancellationToken)
            => await ReadAsync(reader, cancellationToken);
    }

    private protected sealed override ValueTask WriteAsObject(bool async, PgWriter writer, object value, CancellationToken cancellationToken = default)
    {
        if (async)
            return WriteAsync(writer, (T)value, cancellationToken);

        Write(writer, (T)value);
        return new();
    }
}

// Base class for converters that know their binary size up front.
abstract class PgFixedBinarySizeConverter<T> : PgConverter<T>
{
    protected PgFixedBinarySizeConverter(bool extendedDbNullPredicate = false) : base(extendedDbNullPredicate) { }

    // public sealed override bool CanConvert(DataFormat format) => format is DataFormat.Binary;
    public sealed override bool HasFixedSize(DataFormat format) => format is DataFormat.Binary;
    protected abstract byte BinarySize { get; }

    public override ValueSize GetSize(SizeContext context, T value, ref object? writeState)
        => context.Format is DataFormat.Binary ? BinarySize : throw new NotSupportedException();

    protected abstract T? ReadCore(PgReader reader);

    protected virtual ValueTask<T?> ReadCoreAsync(PgReader reader, CancellationToken cancellationToken = default)
        => new(ReadCore(reader));

    // By default ReadAsync delegates to Read, so any text read would be able to be handled too.
    public sealed override T? Read(PgReader reader)
    {
        if (reader.Format is DataFormat.Binary && reader.Remaining < BinarySize)
            reader.WaitForData(reader.ByteCount);

        return ReadCore(reader);

    }

    public override ValueTask<T?> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
    {
        if (reader.Format is DataFormat.Binary && reader.Remaining < BinarySize)
            return Core(reader, cancellationToken);

        return ReadCoreAsync(reader, cancellationToken);

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        async ValueTask<T?> Core(PgReader reader, CancellationToken cancellationToken)
        {
            await reader.WaitForDataAsync(BinarySize, cancellationToken);
            return await ReadCoreAsync(reader, cancellationToken);
        }
    }
}


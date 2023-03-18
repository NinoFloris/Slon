using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg;

// Lives here to prevent generic IL metadata duplication.
enum DbNullPredicate: byte
{
    /// Value is null
    Default,
    /// Value is null or DBNull
    Polymorphic,
    /// Value is null or *user code*
    Extended
}

abstract class PgConverter
{
    private protected PgConverter() { }

    internal abstract Type TypeToConvert { get; }
    internal abstract bool IsDbNullable { get; }
    internal abstract bool IsDbNullValueAsObject([NotNullWhen(false)]object? value);

    public virtual bool CanConvert(DataFormat format) => format is DataFormat.Binary;

    /// This method returns true when GetSize can be called with a default value for the type and the given representation without throwing.
    public virtual bool HasFixedSize(DataFormat format) => false;

    internal abstract object? ReadAsObject(PgReader reader);
    internal abstract ValueTask<object?> ReadAsObjectAsync(PgReader reader, CancellationToken cancellationToken = default);

    internal abstract ValueSize GetSizeAsObject(SizeContext context, object value, [NotNullIfNotNull(nameof(writeState))]ref object? writeState);
    internal abstract void WriteAsObject(PgWriter writer, object value);
    internal abstract ValueTask WriteAsObjectAsync(PgWriter writer, object value, CancellationToken cancellationToken = default);

    private protected static DbNullPredicate GetDbNullPredicate(DbNullPredicate dbNullPredicate, Type type)
        => dbNullPredicate switch
        {
            DbNullPredicate.Polymorphic when type != typeof(object) => throw new ArgumentException(nameof(dbNullPredicate), $"{nameof(DbNullPredicate.Polymorphic)} can only be used with a 'System.Object' type."),
            DbNullPredicate.Default when !type.IsValueType && (type == typeof(object) || type == typeof(DBNull)) => DbNullPredicate.Polymorphic,
            _ => dbNullPredicate
        };

    private protected static DbNullPredicate FromDelegatedDbNullPredicate(DbNullPredicate delegatedPredicate, Type type)
        => delegatedPredicate switch
        {
            // If the DbNullPredicate for the given type would not be upgraded to Polymorphic we keep the result at Default instead of copying the delegated value.
            DbNullPredicate.Polymorphic when GetDbNullPredicate(DbNullPredicate.Default, type) is DbNullPredicate.Default => DbNullPredicate.Default,
            _ => delegatedPredicate
        };
}

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

abstract class PgConverter<T> : PgConverter
{
    protected internal DbNullPredicate DbNullPredicate { get; }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="dbNullPredicate">
    /// when T is object or DBNull <paramref name="dbNullPredicate"/> is automatically upgraded from Default to Polymorphic.
    /// </param>
    protected PgConverter(DbNullPredicate dbNullPredicate = DbNullPredicate.Default)
        => DbNullPredicate = GetDbNullPredicate(dbNullPredicate, typeof(T));

    protected virtual bool IsDbNull(T? value)
        => DbNullPredicate is DbNullPredicate.Polymorphic ? value is DBNull : throw new NotImplementedException();

    public bool IsDbNullValue([NotNullWhen(false)] T? value)
        => value is null || (DbNullPredicate is not DbNullPredicate.Default && IsDbNull(value));

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

    internal sealed override bool IsDbNullable => default(T) is null || DbNullPredicate is not DbNullPredicate.Default;

    // Object null semantics as follows, if T is a struct (so excluding nullable) report false for null values, don't throw on the cast.
    // As a result this creates symmetry with IsDbNullValue when we're dealing with a struct T, as it cannot be passed null at all.
    internal sealed override bool IsDbNullValueAsObject([NotNullWhen(false)]object? value)
        => (default(T) is null || value is not null) && IsDbNullValue((T?)value);

    internal sealed override object? ReadAsObject(PgReader reader)
        => Read(reader);

    internal sealed override ValueTask<object?> ReadAsObjectAsync(PgReader reader, CancellationToken cancellationToken = default)
    {
        var task = ReadAsync(reader, cancellationToken);
        return task.IsCompletedSuccessfully ? new(task.GetAwaiter().GetResult()) : Core(task);

        static async ValueTask<object?> Core(ValueTask<T?> task) => await task;
    }

    internal sealed override ValueSize GetSizeAsObject(SizeContext context, object value, [NotNullIfNotNull(nameof(writeState))]ref object? writeState)
        => GetSize(context, (T)value, ref writeState);

    internal sealed override void WriteAsObject(PgWriter writer, object value)
        => Write(writer, (T)value);

    internal sealed override ValueTask WriteAsObjectAsync(PgWriter writer, object value, CancellationToken cancellationToken = default)
        => WriteAsync(writer, (T)value, cancellationToken);
}

// Base class for converters that know their binary size up front.
abstract class PgFixedBinarySizeConverter<T> : PgConverter<T>
{
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


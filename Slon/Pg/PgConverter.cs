using System;
using System.Diagnostics.CodeAnalysis;
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

    public abstract bool IsDbNullable { get; }
    internal abstract bool IsDbNullValueAsObject([NotNullWhen(false)]object? value, PgConverterOptions options);

    public virtual bool CanConvert(DataRepresentation representation) => representation is DataRepresentation.Binary;

    /// This method returns true when GetSize can be called with a default value for the type and the given representation without throwing.
    public virtual bool HasFixedSize(DataRepresentation representation) => false;

    internal abstract object? ReadAsObject(PgReader reader, PgConverterOptions options);
    internal abstract ValueTask<object?> ReadAsObjectAsync(PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default);

    internal abstract ValueSize GetSizeAsObject(object value, [NotNullIfNotNull(nameof(writeState))]ref object? writeState, SizeContext context, PgConverterOptions options);
    internal abstract void WriteAsObject(PgWriter writer, object value, PgConverterOptions options);
    internal abstract ValueTask WriteAsObjectAsync(PgWriter writer, object value, PgConverterOptions options, CancellationToken cancellationToken = default);

    private protected static DbNullPredicate GetDbNullPredicate(DbNullPredicate dbNullPredicate, Type type)
    {
        if (dbNullPredicate is DbNullPredicate.Default && !type.IsValueType && (type == typeof(object) || type == typeof(DBNull)))
            return DbNullPredicate.Polymorphic;

        return dbNullPredicate;
    }

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
    public SizeContext(DataRepresentation representation, int bufferLength)
    {
        Representation = representation;
        BufferLength = bufferLength;
    }

    public DataRepresentation Representation { get; }
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

    protected virtual bool IsDbNull(T? value, PgConverterOptions options)
        => DbNullPredicate is DbNullPredicate.Polymorphic ? value is DBNull : throw new NotImplementedException();

    public sealed override bool IsDbNullable => default(T) is null || DbNullPredicate is not DbNullPredicate.Default;

    public bool IsDbNullValue([NotNullWhen(false)] T? value, PgConverterOptions options)
        => value is null || (DbNullPredicate is not DbNullPredicate.Default && IsDbNull(value, options));

    public abstract T? Read(PgReader reader, PgConverterOptions options);

    public virtual ValueTask<T?> ReadAsync(PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default)
        => new(Read(reader, options));

    public abstract ValueSize GetSize(T value, [NotNullIfNotNull(nameof(writeState))]ref object? writeState, SizeContext context, PgConverterOptions options);
    public abstract void Write(PgWriter writer, T value, PgConverterOptions options);

    public virtual ValueTask WriteAsync(PgWriter writer, T value, PgConverterOptions options, CancellationToken cancellationToken = default)
    {
        Write(writer, value, options);
        return new();
    }

    internal sealed override Type TypeToConvert => typeof(T);

    // Object null semantics as follows, if T is a struct (so excluding nullable) report false for null values, don't throw on the cast.
    // As a result this creates symmetry with IsDbNullValue when we're dealing with a struct T, as it cannot be passed null at all.
    internal sealed override bool IsDbNullValueAsObject([NotNullWhen(false)]object? value, PgConverterOptions options)
        => (default(T) is null || value is not null) && IsDbNullValue((T?)value, options);

    internal sealed override object? ReadAsObject(PgReader reader, PgConverterOptions options)
        => Read(reader, options);

    internal sealed override ValueTask<object?> ReadAsObjectAsync(PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default)
    {
        var task = ReadAsync(reader, options, cancellationToken);
        return task.IsCompletedSuccessfully ? new(task.GetAwaiter().GetResult()) : Core(task);

        static async ValueTask<object?> Core(ValueTask<T?> task) => await task;
    }

    internal sealed override ValueSize GetSizeAsObject(object value, [NotNullIfNotNull(nameof(writeState))]ref object? writeState, SizeContext context, PgConverterOptions options)
        => GetSize((T)value, ref writeState, context, options);

    internal sealed override void WriteAsObject(PgWriter writer, object value, PgConverterOptions options)
        => Write(writer, (T)value, options);

    internal sealed override ValueTask WriteAsObjectAsync(PgWriter writer, object value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => WriteAsync(writer, (T)value, options, cancellationToken);
}

// Base class for converters that know their binary size up front. Helps reduce IO and sizing costs.
abstract class FixedBinarySizePgConverter<T> : PgConverter<T>
{
    public sealed override bool HasFixedSize(DataRepresentation representation) => representation is DataRepresentation.Binary;
    protected abstract byte BinarySize { get; }

    public override ValueSize GetSize(T value, ref object? writeState, SizeContext context, PgConverterOptions options)
        => context.Representation is DataRepresentation.Binary ? BinarySize : throw new NotSupportedException();

    // TODO ensure BinarySize (if reader.Representation is Binary)
    public sealed override ValueTask<T?> ReadAsync(PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default)
        => new(Read(reader, options));

    public sealed override ValueTask WriteAsync(PgWriter writer, T value, PgConverterOptions options, CancellationToken cancellationToken = default)
    {
        Write(writer, value, options);
        return new();
    }
}

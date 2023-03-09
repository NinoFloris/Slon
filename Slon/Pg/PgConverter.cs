using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Slon.Protocol;

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

// TODO replace SequenceReader<byte> with something that captures its functionality and byteCount (and likely, internalizes async reading, and nullability reading).
abstract class PgConverter
{
    internal PgConverter() { }

    public virtual bool IsDbNullable => throw new NotSupportedException();
    internal abstract bool IsDbNullValueAsObject([NotNullWhen(false)]object? value, PgConverterOptions options);

    public virtual bool CanConvert(DataRepresentation representation) => representation is DataRepresentation.Binary;

    internal abstract ReadStatus ReadAsObject(ref SequenceReader<byte> reader, int byteCount, out object? value, PgConverterOptions options);

    internal abstract SizeResult GetSizeAsObject(object value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options);
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

    public abstract ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out T value, PgConverterOptions options);

    public abstract SizeResult GetSize(T value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options);
    public abstract void Write(PgWriter writer, T value, PgConverterOptions options);

    public virtual ValueTask WriteAsync(PgWriter writer, T value, PgConverterOptions options, CancellationToken cancellationToken = default)
    {
        Write(writer, value, options);
        return new ValueTask();
    }

    // Object null semantics as follows, if T is a struct (so excluding nullable) report false for null values, don't throw on the cast.
    // As a result this creates symmetry with IsDbNullValue when we're dealing with a struct T, as it cannot be passed null at all.
    internal sealed override bool IsDbNullValueAsObject([NotNullWhen(false)]object? value, PgConverterOptions options)
        => (default(T) is null || value is not null) && IsDbNullValue((T?)value, options);

    internal sealed override ReadStatus ReadAsObject(ref SequenceReader<byte> reader, int byteCount, out object? value, PgConverterOptions options)
    {
        var status = Read(ref reader, byteCount, out var typedValue, options);
        value = status is ReadStatus.Done ? typedValue : null;
        return status;
    }

    internal sealed override SizeResult GetSizeAsObject(object value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options)
        => GetSize((T)value, bufferLength, ref writeState, representation, options);

    internal sealed override void WriteAsObject(PgWriter writer, object value, PgConverterOptions options)
        => Write(writer, (T)value, options);

    internal sealed override ValueTask WriteAsObjectAsync(PgWriter writer, object value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => WriteAsync(writer, (T)value, options, cancellationToken);
}

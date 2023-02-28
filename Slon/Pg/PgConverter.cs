using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Slon.Protocol;

namespace Slon.Pg;

// TODO replace SequenceReader<byte> with something that captures its functionality and byteCount (and likely, internalizes async reading, and nullability reading).
abstract class PgConverter
{
    internal PgConverter() {}

    public virtual bool IsDbNullable => true; // All objects are by default.

    internal abstract bool IsDbNullValueAsObject([NotNullWhen(false)]object? value, PgConverterOptions options);

    public virtual bool CanConvert(DataRepresentation representation) => representation is DataRepresentation.Binary;

    internal abstract ReadStatus ReadAsObject(ref SequenceReader<byte> reader, int byteCount, out object? value, PgConverterOptions options);

    internal abstract SizeResult GetSizeAsObject(object value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options);
    internal abstract void WriteAsObject(PgWriter writer, object value, PgConverterOptions options);
    internal abstract ValueTask WriteAsObjectAsync(PgWriter writer, object value, PgConverterOptions options, CancellationToken cancellationToken = default);
}

abstract class PgConverter<T> : PgConverter
{
    static bool IsStructType => typeof(T).IsValueType && default(T) != null;

    // We support custom extended db null semantics but we enable this via a delegate instead of a virtual method as it's quite uncommon.
    // This is also important for perf as we want the default semantics (almost all of the converters) to be non virtual and inlineable.
    // Going with a virtual method would necessitate some virtual property (which we'd cache the result of) to return whether we should call the virtual method.
    // It does not particularly seem better DX to have a user override two virtual methods compared to pointing a delegate to a method.
    // Having something low-level like this would have been a nice - just right - alternative... https://github.com/dotnet/runtime/issues/12760
    protected Func<PgConverter<T>, T, PgConverterOptions, bool>? IsDbNull { get; init; }
    public sealed override bool IsDbNullable => !IsStructType || IsDbNull is not null;

    public bool IsDbNullValue([NotNullWhen(false)] T? value, PgConverterOptions options)
        => value is null or DBNull || IsDbNull?.Invoke(this, value, options) is true;

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
        => (value is not null || !IsStructType) && IsDbNullValue((T?)value, options);

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

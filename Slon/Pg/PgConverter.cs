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

    internal virtual bool IsTypeDbNullable => true; // All objects are by default.

    internal abstract bool IsDbNullValueAsObject([NotNullWhen(false)]object? value, PgConverterOptions options);

    // Overridden for text only converters.
    public virtual bool CanConvert => true;

    internal abstract ReadStatus ReadAsObject(ref SequenceReader<byte> reader, int byteCount, out object? value, PgConverterOptions options);

    internal abstract SizeResult GetSizeAsObject(object value, int bufferLength, ref object? writeState, PgConverterOptions options);
    internal abstract void WriteAsObject(PgWriter writer, object value, PgConverterOptions options);
    internal abstract ValueTask WriteAsObjectAsync(PgWriter writer, object value, PgConverterOptions options, CancellationToken cancellationToken = default);

    // Overridden when the textual representation is supported.
    public virtual bool CanTextConvert => false;

    internal abstract ReadStatus ReadTextAsObject(ref SequenceReader<byte> reader, int byteCount, out object? value, PgConverterOptions options);

    internal abstract SizeResult GetTextSizeAsObject(object value, int bufferLength, ref object? writeState, PgConverterOptions options);
    internal abstract void WriteTextAsObject(PgWriter writer, object value, PgConverterOptions options);
    internal abstract ValueTask WriteTextAsObjectAsync(PgWriter writer, object value, PgConverterOptions options, CancellationToken cancellationToken = default);
}

abstract class PgConverter<T> : PgConverter
{
    static readonly bool IsStructType = typeof(T).IsValueType && Nullable.GetUnderlyingType(typeof(T)) is null;

    // We support custom extended db null semantics but we enable this via a delegate instead of a virtual method as it's quite uncommon.
    // This is also important for perf as we want the default semantics (almost all of the converters) to be non virtual and inlineable.
    // Going with a virtual method would necessitate some virtual property (which we'd cache the result of) to return whether we should call the virtual method.
    // It does not particularly seem better DX to have a user override two virtual methods compared to pointing a delegate to a method.
    // Having something low-level like this would have been a nice - just right - alternative... https://github.com/dotnet/runtime/issues/12760
    protected Func<PgConverter<T>, T, PgConverterOptions, bool>? IsDbNull { get; init; }
    internal sealed override bool IsTypeDbNullable => !IsStructType || IsDbNull is not null;

    public bool IsDbNullValue([NotNullWhen(false)] T? value, PgConverterOptions options)
        => value is null or DBNull || IsDbNull?.Invoke(this, value, options) is true;

    public abstract ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out T value, PgConverterOptions options);

    public abstract SizeResult GetSize(T value, int bufferLength, ref object? writeState, PgConverterOptions options);
    public abstract void Write(PgWriter writer, T value, PgConverterOptions options);

    public virtual ValueTask WriteAsync(PgWriter writer, T value, PgConverterOptions options, CancellationToken cancellationToken = default)
    {
        Write(writer, value, options);
        return new ValueTask();
    }

    public virtual ReadStatus ReadText(ref SequenceReader<byte> reader, int byteCount, out T value, PgConverterOptions options)
        => throw new NotSupportedException();

    public virtual SizeResult GetTextSize(T value, int bufferLength, ref object? writeState, PgConverterOptions options)
        => throw new NotSupportedException();

    public virtual void WriteText(PgWriter writer, T value, PgConverterOptions options)
        => throw new NotSupportedException();

    public virtual ValueTask WriteTextAsync(PgWriter writer, T value, PgConverterOptions options, CancellationToken cancellationToken = default)
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

    internal sealed override SizeResult GetSizeAsObject(object value, int bufferLength, ref object? writeState, PgConverterOptions options)
        => GetSize((T)value, bufferLength, ref writeState, options);

    internal sealed override void WriteAsObject(PgWriter writer, object value, PgConverterOptions options)
        => Write(writer, (T)value, options);

    internal sealed override ValueTask WriteAsObjectAsync(PgWriter writer, object value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => WriteAsync(writer, (T)value, options, cancellationToken);

    internal sealed override SizeResult GetTextSizeAsObject(object value, int bufferLength, ref object? writeState, PgConverterOptions options)
        => GetSize((T)value, bufferLength, ref writeState, options);

    internal sealed override ReadStatus ReadTextAsObject(ref SequenceReader<byte> reader, int byteCount, out object? value, PgConverterOptions options)
    {
        var status = Read(ref reader, byteCount, out var typedValue, options);
        value = status is ReadStatus.Done ? typedValue : null;
        return status;
    }

    internal sealed override void WriteTextAsObject(PgWriter writer, object value, PgConverterOptions options)
        => WriteText(writer, (T)value, options);

    internal sealed override ValueTask WriteTextAsObjectAsync(PgWriter writer, object value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => WriteAsync(writer, (T)value, options, cancellationToken);
}

enum SizeResultKind: byte
{
    Size,
    FixedSize,
    UpperBound,
    Unknown
}

readonly record struct SizeResult
{
    readonly int _byteCount;

    SizeResult(int byteCount, SizeResultKind kind)
    {
        _byteCount = byteCount;
        Kind = kind;
    }

    public int? Value
    {
        get
        {
            if (Kind is SizeResultKind.Unknown)
                return null;

            return _byteCount;
        }
    }
    public SizeResultKind Kind { get; }

    public static SizeResult Create(int byteCount) => new(byteCount, SizeResultKind.Size);
    public static SizeResult Create(int byteCount, bool fixedSize) => new(byteCount, fixedSize ? SizeResultKind.FixedSize : SizeResultKind.Size);
    public static SizeResult CreateUpperBound(int byteCount) => new(byteCount, SizeResultKind.UpperBound);
    public static SizeResult Unknown => new(default, SizeResultKind.Unknown);
    public static SizeResult Zero => new(0, SizeResultKind.Size);

    public SizeResult Combine(SizeResult result)
    {
        if (Kind is SizeResultKind.Unknown || result.Kind is SizeResultKind.Unknown)
            return this;

        if (Kind is SizeResultKind.UpperBound || result.Kind is SizeResultKind.UpperBound)
            return CreateUpperBound(_byteCount + result._byteCount);

        if (Kind is SizeResultKind.Size || result.Kind is SizeResultKind.Size)
            return Create(_byteCount + result._byteCount);

        return Create(_byteCount + result._byteCount, fixedSize: true);
    }
}

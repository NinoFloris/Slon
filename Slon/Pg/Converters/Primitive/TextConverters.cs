using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Slon.Protocol;

namespace Slon.Pg.Converters;

sealed class ReadOnlyMemoryTextConverter: PgConverter<ReadOnlyMemory<char>>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out ReadOnlyMemory<char> value, PgConverterOptions options)
        => ReadCore(ref reader, byteCount, out value, options);

    public override SizeResult GetSize(ReadOnlyMemory<char> value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options)
        => GetSizeCore(value, bufferLength, options);
    public override void Write(PgWriter writer, ReadOnlyMemory<char> value, PgConverterOptions options)
        => WriteCore(false, writer, value, options.TextEncoding, CancellationToken.None).GetAwaiter().GetResult();
    public override ValueTask WriteAsync(PgWriter writer, ReadOnlyMemory<char> value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => WriteCore(true, writer, value, options.TextEncoding, cancellationToken);

    public override bool CanConvert(DataRepresentation representation) => representation is DataRepresentation.Binary or DataRepresentation.Text;

    ReadStatus ReadCore(ref SequenceReader<byte> reader, int byteLength, out ReadOnlyMemory<char> value, PgConverterOptions options)
    {
        if (!reader.TryReadExact(byteLength, out var bytes))
        {
            value = default;
            return ReadStatus.NeedMoreData;
        }

        var rentedArray = ArrayPool<char>.Shared.Rent(options.TextEncoding.GetMaxCharCount(byteLength));

        var count = options.TextEncoding.GetChars(bytes, rentedArray.AsSpan());
        var arrayValue = new char[count];
        Array.Copy(rentedArray, arrayValue, count);
        ArrayPool<char>.Shared.Return(rentedArray);
        value = new ReadOnlyMemory<char>(arrayValue);
        return ReadStatus.Done;
    }

    SizeResult GetSizeCore(ReadOnlyMemory<char> value, int bufferLength, PgConverterOptions options)
    {
        // Save a traverse if it fits, we'll write the used buffer space as its length afterwards.
        var upperBound = options.TextEncoding.GetMaxByteCount(value.Length);
        // Benchmarks indicated sizing 50 chars takes between 6 and 50ns for utf8 depending on ascii/unicode mix.
        // That is fast enough we're not bothering with upper bounds/unknown sizing.
        if (value.Length > 50 && bufferLength >= upperBound)
            return SizeResult.CreateUpperBound(upperBound);

        return SizeResult.Create(options.TextEncoding.GetByteCount(value.Span));
    }

    async ValueTask WriteCore(bool async, PgWriter writer, ReadOnlyMemory<char> value, Encoding encoding, CancellationToken cancellationToken)
    {
        const int chunkSize = 8096;
        var offset = 0;
        Encoder? encoder = null;
        while (offset < value.Length)
        {
            var nextLength = Math.Min(chunkSize, value.Length - offset);
            encoder = writer.WriteTextResumable(value.Span.Slice(offset, nextLength), encoding, encoder);
            offset += nextLength;
            await writer.Flush(async, cancellationToken);
        }
    }
}

sealed class StringTextConverter : ValueConverter<string, ReadOnlyMemory<char>, ReadOnlyMemoryTextConverter>
{
    public StringTextConverter(ReadOnlyMemoryTextConverter effectiveConverter) : base(effectiveConverter) { }
    protected override string ConvertFrom(ReadOnlyMemory<char> value, PgConverterOptions options) => throw new NotSupportedException();
    protected override ReadOnlyMemory<char> ConvertTo(string value, PgConverterOptions options) => value.AsMemory();

    ReadStatus ReadCore(ref SequenceReader<byte> reader, int byteLength, out string value, PgConverterOptions options)
    {
        if (!reader.TryReadExact(byteLength, out var bytes))
        {
            value = null!;
            return ReadStatus.NeedMoreData;
        }

        value = options.TextEncoding.GetString(bytes);
        return ReadStatus.Done;
    }

    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out string value, PgConverterOptions options)
        => ReadCore(ref reader, byteCount, out value, options);
}

sealed class CharArrayTextConverter : ValueConverter<char[], ReadOnlyMemory<char>, ReadOnlyMemoryTextConverter>
{
    public CharArrayTextConverter(ReadOnlyMemoryTextConverter effectiveConverter) : base(effectiveConverter) { }

    protected override char[] ConvertFrom(ReadOnlyMemory<char> value, PgConverterOptions options)
        => MemoryMarshal.TryGetArray(value, out var segment) ? segment.Array! : value.ToArray();
    protected override ReadOnlyMemory<char> ConvertTo(char[] value, PgConverterOptions options) => value.AsMemory();
}

sealed class CharArraySegmentTextConverter : ValueConverter<ArraySegment<char>, ReadOnlyMemory<char>, ReadOnlyMemoryTextConverter>
{
    public CharArraySegmentTextConverter(ReadOnlyMemoryTextConverter effectiveConverter) : base(effectiveConverter) { }

    protected override ArraySegment<char> ConvertFrom(ReadOnlyMemory<char> value, PgConverterOptions options)
        => MemoryMarshal.TryGetArray(value, out var segment) ? new(segment.Array!) : new(value.ToArray());
    protected override ReadOnlyMemory<char> ConvertTo(ArraySegment<char> value, PgConverterOptions options) => value.AsMemory();
}

sealed class CharTextConverter : PgConverter<char>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out char value, PgConverterOptions options)
        => ReadCore(ref reader, byteCount, out value, options);

    public override SizeResult GetSize(char value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options)
        => GetSizeCore(value, bufferLength, options);
    public override void Write(PgWriter writer, char value, PgConverterOptions options)
        => WriteCore(writer, value, options.TextEncoding);

    public override bool CanConvert(DataRepresentation representation) => representation is DataRepresentation.Binary or DataRepresentation.Text;

    ReadStatus ReadCore(ref SequenceReader<byte> reader, int byteLength, out char value, PgConverterOptions options)
    {
        if (!reader.TryReadExact(Math.Min(options.TextEncoding.GetMaxByteCount(1), byteLength), out var bytes))
        {
            value = default;
            return ReadStatus.NeedMoreData;
        }

        Span<char> destination = stackalloc char[1];
        options.TextEncoding.GetChars(bytes, destination);
        value = destination[0];
        return ReadStatus.Done;
    }

    SizeResult GetSizeCore(char value, int bufferLength, PgConverterOptions options)
    {
        Span<char> spanValue = stackalloc char[] { value };
        return SizeResult.Create(options.TextEncoding.GetByteCount(spanValue));
    }

    void WriteCore(PgWriter writer, char value, Encoding encoding)
    {
        Span<char> spanValue = stackalloc char[] { value };
        writer.WriteTextResumable(spanValue, encoding);
    }
}

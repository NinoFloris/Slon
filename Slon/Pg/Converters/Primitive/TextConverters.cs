using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg.Converters;

sealed class ReadOnlyMemoryTextConverter: PgConverter<ReadOnlyMemory<char>>
{
    public override ReadOnlyMemory<char> Read(PgReader reader, PgConverterOptions options)
        => ReadCore(async: false, reader, options).GetAwaiter().GetResult();

    public override ValueTask<ReadOnlyMemory<char>> ReadAsync(PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default)
        => ReadCore(async: true, reader, options);

    public override SizeResult GetSize(ReadOnlyMemory<char> value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options)
    {
        // Benchmarks indicated sizing 50 chars takes between 6 and 50ns for utf8 depending on ascii/unicode mix.
        // That is fast enough we're not bothering with upper bounds/unknown sizing for those smaller strings.
        if (value.Length > 50 && bufferLength >= value.Length)
        {
            var upperBound = options.TextEncoding.GetMaxByteCount(value.Length);
            // Saves a traverse if it fits, we'll write the used buffer space as its length afterwards.
            if (bufferLength > upperBound)
                return SizeResult.CreateUpperBound(upperBound);
        }

        return SizeResult.Create(options.TextEncoding.GetByteCount(value.Span));
    }

    public override void Write(PgWriter writer, ReadOnlyMemory<char> value, PgConverterOptions options)
        => WriteCore(async: false, writer, value, options.TextEncoding, CancellationToken.None).GetAwaiter().GetResult();
    public override ValueTask WriteAsync(PgWriter writer, ReadOnlyMemory<char> value, PgConverterOptions options, CancellationToken cancellationToken = default)
        => WriteCore(async: true, writer, value, options.TextEncoding, cancellationToken);

    public override bool CanConvert(DataRepresentation representation) => representation is DataRepresentation.Binary or DataRepresentation.Text;

    ValueTask<ReadOnlyMemory<char>> ReadCore(bool async, PgReader reader, PgConverterOptions options)
    {
        var bytes = reader.ReadExact(reader.ByteCount);
        var rentedArray = ArrayPool<char>.Shared.Rent(options.TextEncoding.GetMaxCharCount(reader.ByteCount));

        var count = options.TextEncoding.GetChars(bytes, rentedArray.AsSpan());
        var arrayValue = new char[count];
        Array.Copy(rentedArray, arrayValue, count);
        ArrayPool<char>.Shared.Return(rentedArray);
        return new(arrayValue);
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

sealed class StringTextConverter : ValueConverter<string, ReadOnlyMemory<char>>
{
    public StringTextConverter(PgConverter<ReadOnlyMemory<char>> effectiveConverter) : base(effectiveConverter) { }
    protected override string ConvertFrom(ReadOnlyMemory<char> value, PgConverterOptions options) => throw new NotSupportedException();
    protected override ReadOnlyMemory<char> ConvertTo(string value, PgConverterOptions options) => value.AsMemory();

    public override string Read(PgReader reader, PgConverterOptions options)
        => ReadCore(async: false, reader, options).GetAwaiter().GetResult();

    public override ValueTask<string> ReadAsync(PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default)
        => ReadCore(async: true, reader, options, cancellationToken);

    ValueTask<string> ReadCore(bool async, PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default)
        => new(options.TextEncoding.GetString(reader.ReadExact(reader.ByteCount)));
}

sealed class CharArrayTextConverter : ValueConverter<char[], ReadOnlyMemory<char>>
{
    public CharArrayTextConverter(PgConverter<ReadOnlyMemory<char>> effectiveConverter) : base(effectiveConverter) { }

    protected override char[] ConvertFrom(ReadOnlyMemory<char> value, PgConverterOptions options)
        => MemoryMarshal.TryGetArray(value, out var segment) ? segment.Array! : value.ToArray();
    protected override ReadOnlyMemory<char> ConvertTo(char[] value, PgConverterOptions options) => value.AsMemory();
}

sealed class CharArraySegmentTextConverter : ValueConverter<ArraySegment<char>, ReadOnlyMemory<char>>
{
    public CharArraySegmentTextConverter(PgConverter<ReadOnlyMemory<char>> effectiveConverter) : base(effectiveConverter) { }

    protected override ArraySegment<char> ConvertFrom(ReadOnlyMemory<char> value, PgConverterOptions options)
        => MemoryMarshal.TryGetArray(value, out var segment) ? new(segment.Array!) : new(value.ToArray());
    protected override ReadOnlyMemory<char> ConvertTo(ArraySegment<char> value, PgConverterOptions options) => value.AsMemory();
}

sealed class CharTextConverter : PgConverter<char>
{
    public override bool CanConvert(DataRepresentation representation) => representation is DataRepresentation.Binary or DataRepresentation.Text;

    public override char Read(PgReader reader, PgConverterOptions options)
    {
        var bytes = reader.ReadExact(Math.Min(options.TextEncoding.GetMaxByteCount(1), reader.ByteCount));
        Span<char> destination = stackalloc char[1];
        options.TextEncoding.GetChars(bytes, destination);
        return destination[0];
    }

    public override SizeResult GetSize(char value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options)
    {
        Span<char> spanValue = stackalloc char[] { value };
        return SizeResult.Create(options.TextEncoding.GetByteCount(spanValue));
    }

    public override void Write(PgWriter writer, char value, PgConverterOptions options)
    {
        Span<char> spanValue = stackalloc char[] { value };
        writer.WriteTextResumable(spanValue, options.TextEncoding);
    }
}

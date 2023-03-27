using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg.Converters;

sealed class ReadOnlyMemoryTextConverter: PgStreamingConverter<ReadOnlyMemory<char>>
{
    readonly Encoding _textEncoding;
    public ReadOnlyMemoryTextConverter(PgConverterOptions options)
        => _textEncoding = options.TextEncoding;

    public override ReadOnlyMemory<char> Read(PgReader reader)
        => ReadCore(async: false, reader).GetAwaiter().GetResult();

    public override Task<ReadOnlyMemory<char>> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => ReadCore(async: true, reader);

    public override ValueSize GetSize(ref SizeContext context, ReadOnlyMemory<char> value)
    {
        // Benchmarks indicated sizing 50 chars takes between 6 and 50ns for utf8 depending on ascii/unicode mix.
        // That is fast enough we're not bothering with upper bounds/unknown sizing for those smaller strings.
        if (value.Length > 50 && context.BufferLength >= value.Length)
        {
            var upperBound = _textEncoding.GetMaxByteCount(value.Length);
            // Saves a traverse if it fits, we'll write the used buffer space as its length afterwards.
            if (context.BufferLength > upperBound)
                return ValueSize.CreateUpperBound(upperBound);
        }

        return _textEncoding.GetByteCount(value.Span);
    }

    public override void Write(PgWriter writer, ReadOnlyMemory<char> value)
        => WriteCore(async: false, writer, value, _textEncoding, CancellationToken.None).GetAwaiter().GetResult();
    public override ValueTask WriteAsync(PgWriter writer, ReadOnlyMemory<char> value, CancellationToken cancellationToken = default)
        => WriteCore(async: true, writer, value, _textEncoding, cancellationToken);

    public override bool CanConvert(DataFormat format, out bool fixedSize)
    {
        fixedSize = false;
        return format is DataFormat.Binary or DataFormat.Text;
    }

    Task<ReadOnlyMemory<char>> ReadCore(bool async, PgReader reader)
    {
        var bytes = reader.ReadExact(reader.ByteCount);
        var rentedArray = ArrayPool<char>.Shared.Rent(_textEncoding.GetMaxCharCount(reader.ByteCount));

        var count = _textEncoding.GetChars(bytes, rentedArray.AsSpan());
        var arrayValue = new char[count];
        Array.Copy(rentedArray, arrayValue, count);
        ArrayPool<char>.Shared.Return(rentedArray);
        return Task.FromResult<ReadOnlyMemory<char>>(arrayValue.AsMemory());
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
    readonly Encoding _textEncoding;
    public StringTextConverter(PgConverter<ReadOnlyMemory<char>> effectiveConverter, PgConverterOptions options) : base(effectiveConverter)
    {
        _textEncoding = options.TextEncoding;
    }
    protected override string ConvertFrom(ReadOnlyMemory<char> value) => throw new NotSupportedException();
    protected override ReadOnlyMemory<char> ConvertTo(string value) => value.AsMemory();

    public override string Read(PgReader reader)
        => ReadCore(async: false, reader).GetAwaiter().GetResult();

    public override Task<string> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => ReadCore(async: true, reader, cancellationToken);

    Task<string> ReadCore(bool async, PgReader reader, CancellationToken cancellationToken = default)
        => Task.FromResult(_textEncoding.GetString(reader.ReadExact(reader.ByteCount)));
}

sealed class CharArrayTextConverter : ValueConverter<char[], ReadOnlyMemory<char>>
{
    public CharArrayTextConverter(PgConverter<ReadOnlyMemory<char>> effectiveConverter) : base(effectiveConverter) { }

    protected override char[] ConvertFrom(ReadOnlyMemory<char> value)
    {
        if (MemoryMarshal.TryGetArray(value, out var segment))
            // We need to return an exact sized array.
            return segment.Array!.Length > segment.Count ? value.ToArray() : segment.Array!;

        return value.ToArray();
    }
    protected override ReadOnlyMemory<char> ConvertTo(char[] value) => value.AsMemory();
}

sealed class CharArraySegmentTextConverter : ValueConverter<ArraySegment<char>, ReadOnlyMemory<char>>
{
    public CharArraySegmentTextConverter(PgConverter<ReadOnlyMemory<char>> effectiveConverter) : base(effectiveConverter) { }

    protected override ArraySegment<char> ConvertFrom(ReadOnlyMemory<char> value)
        => MemoryMarshal.TryGetArray(value, out var segment) ? new(segment.Array!) : new(value.ToArray());
    protected override ReadOnlyMemory<char> ConvertTo(ArraySegment<char> value) => value.AsMemory();
}

sealed class CharTextConverter : PgBufferedConverter<char>
{
    readonly Encoding _textEncoding;
    public CharTextConverter(PgConverterOptions options) => _textEncoding = options.TextEncoding;

    public override bool CanConvert(DataFormat format, out bool fixedSize)
    {
        fixedSize = format is DataFormat.Binary;
        return format is DataFormat.Binary or DataFormat.Text;
    }

    protected override char ReadCore(PgReader reader)
    {
        var bytes = reader.ReadExact(Math.Min(_textEncoding.GetMaxByteCount(1), reader.ByteCount));
        Span<char> destination = stackalloc char[1];
        _textEncoding.GetChars(bytes, destination);
        return destination[0];
    }

    public override ValueSize GetSize(ref SizeContext context, char value)
    {
        if (context.Format is DataFormat.Binary)
            return sizeof(char);

        Span<char> spanValue = stackalloc char[] { value };
        return _textEncoding.GetByteCount(spanValue);
    }

    public override void Write(PgWriter writer, char value)
    {
        Span<char> spanValue = stackalloc char[] { value };
        writer.WriteTextResumable(spanValue, _textEncoding);
    }
}

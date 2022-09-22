using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines;

static class PgEncoding
{
    public static UTF8Encoding UTF8 { get; } = new(false, true);
    public static UTF8Encoding RelaxedUTF8 { get; } = new(false, false);
}

static class MessageWriter
{
    public static int GetCStringByteCount(string value) => PgEncoding.UTF8.GetByteCount(value.AsSpan()) + 1;
    public const int IntByteCount = sizeof(int);
    public const int ShortByteCount = sizeof(short);
    public const int ByteByteCount = sizeof(byte);
}

// MessageWriter weighs 36 bytes if T is a ref type.
// Might be worth passing by ref.
struct MessageWriter<T> where T : IBufferWriter<byte>
{
    BufferWriter<T> _writer;

    public MessageWriter(T writer)
    {
        _writer = new BufferWriter<T>(writer);
    }

    public T Writer => _writer.Output;

    public void WriteRaw(ReadOnlySpan<byte> value)
    {
        _writer.Write(value);
    }

    public void WriteShort(short value)
    {
        BinaryPrimitives.WriteInt16BigEndian(_writer.Span, value);
        _writer.Advance(sizeof(short));
    }

    public void WriteInt(int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(_writer.Span, value);
        _writer.Advance(sizeof(int));
    }

    public void WriteCString(string value)
    {
        if (value is not "")
            _writer.WriteEncoded(value.AsSpan(), PgEncoding.UTF8);
        _writer.WriteByte(0);
    }

    public async ValueTask<FlushResult> WriteHugeCStringAsync(string value)
    {
        var offset = 0;
        Encoder? encoder = null;
        var writer = _writer;
        while (offset < value.Length)
        {
            WriteChunk();
            await FlushAsync();
        }
        _writer.WriteByte(0);
        return await FlushAsync();

        void WriteChunk()
        {
            var span = value.AsSpan().Slice(offset, Math.Min(value.Length - offset, writer.Span.Length));
            encoder = writer.WriteEncoded(span, PgEncoding.UTF8, encoder);
            offset += span.Length;
        }
    }

    public void WriteByte(byte value)
        => _writer.WriteByte(value);

    /// <summary>
    /// Calls <see cref="IBufferWriter{T}.Advance(int)"/> on the underlying writer
    /// with the number of uncommitted bytes.
    /// </summary>
    public void Commit()
    {
        _writer.Commit();
    }

    /// <summary>
    /// To be called when the Writer was writen to outside of MessageWriter apis
    /// This will bring the MessageWriter in sync again with the underlying writer so it can be called for further use.
    /// </summary>
    /// <param name="count"></param>
    public void AdvanceCommitted(long count)
    {
        _writer.AdvanceCommitted(count);
    }

    long _flushedBytes;
    public long UnflushedBytes => _writer.BufferedBytes + _writer.BytesCommitted - _flushedBytes;

    /// Commit and Flush on the underlying writer if threshold is reached.
    /// Commit is always executed, independent of the flush threshold being reached.
    public ValueTask<FlushResult> FlushAsync(long flushThreshold = -1, CancellationToken cancellationToken = default)
    {
        if (Writer is not IFlushableBufferWriter<byte>)
            throw new NotSupportedException("Underlying writer is not flushable");

        Commit();

        if (flushThreshold != -1 && flushThreshold > UnflushedBytes)
            return new ValueTask<FlushResult>(new FlushResult());

        _flushedBytes = _writer.BytesCommitted;

        return ((IFlushableBufferWriter<byte>)Writer).FlushAsync(cancellationToken);
    }
}

using System;
using System.Buffers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines.Protocol;

static class PgEncoding
{
    public static readonly UTF8Encoding UTF8 = new(false, true);
    public static readonly UTF8Encoding RelaxedUTF8 = new(false, false);
}

static class MessageWriter
{
    public static int GetCStringByteCount(string value) =>
        value is "" ? 1 : PgEncoding.UTF8.GetByteCount(value.AsSpan()) + 1;
    public const int IntByteCount = sizeof(int);
    public const int ShortByteCount = sizeof(short);
    public const int ByteByteCount = sizeof(byte);
}

class MessageWriter<T> where T : IBufferWriter<byte>
{
    BufferWriter<T> _writer;
    readonly FlushControl _flushControl;

    public MessageWriter(T writer, FlushControl flushControl)
    {
        _writer = new BufferWriter<T>(writer);
        _flushControl = flushControl;
        AdvisoryFlushThreshold = _flushControl.FlushThreshold < AdvisoryFlushThreshold ? _flushControl.FlushThreshold : BufferWriter.DefaultCommitThreshold;
    }

    public int AdvisoryFlushThreshold { get; }

    public void WriteRaw(ReadOnlySpan<byte> value) => _writer.Write(value);
    public void WriteByte(byte value) => _writer.WriteByte(value);
    public void WriteShort(short value) => _writer.WriteShort(value);
    public void WriteInt(int value) => _writer.WriteInt(value);
    public void WriteCString(string value) => _writer.WriteCString(value);

    public async ValueTask<FlushResult> WriteHugeCStringAsync(string value, CancellationToken cancellationToken = default)
    {
        var offset = 0;
        Encoder? encoder = null;
        var writer = Writer;
        while (offset < value.Length)
        {
            WriteChunk();
            await FlushAsync(cancellationToken);
        }
        _writer.WriteByte(0);
        return await FlushAsync(cancellationToken);

        void WriteChunk()
        {
            var span = value.AsSpan().Slice(offset, Math.Min(value.Length - offset, writer.Span.Length));
            encoder = writer.WriteEncoded(span, PgEncoding.UTF8, encoder);
            offset += span.Length;
        }
    }

    public long BufferedBytes => Writer.BufferedBytes;
    public long BytesCommitted => Writer.BytesCommitted;
    public long UnflushedBytes => _flushControl.UnflushedBytes + Writer.BufferedBytes;

    internal ref BufferWriter<T> Writer => ref _writer;

    ValueTask<FlushResult> FlushAsyncCore(bool observeFlushThreshold, CancellationToken cancellationToken)
    {
        _writer.Commit();
        return _flushControl.FlushAsync(observeFlushThreshold, cancellationToken);
    }

    /// Commit and Flush on the underlying writer if threshold is reached.
    /// Commit is always executed, independent of the flush threshold being reached.
    public ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
        => FlushAsyncCore(observeFlushThreshold: true, cancellationToken);

    public ValueTask<FlushResult> FlushAsync(bool observeFlushThreshold, CancellationToken cancellationToken = default)
        => FlushAsyncCore(observeFlushThreshold, cancellationToken);

    internal void Reset()
    {
        if (Writer.BufferedBytes > 0)
            throw new InvalidOperationException("Resetting writer while there are still buffered bytes.");

        _writer = new BufferWriter<T>(Writer.Output);
    }
}

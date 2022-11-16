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
    public static int GetCStringByteCount(string value)
        => value.Length != 0 ? PgEncoding.UTF8.GetByteCount(value) + 1 : 1;
    public const int IntByteCount = sizeof(int);
    public const int ShortByteCount = sizeof(short);
    public const int ByteByteCount = sizeof(byte);
    // About the default MTU payload size, not sure how much, if any, it helps.
    public const int DefaultAdvisoryFlushThreshold = 1450;
}

class MessageWriter<TWriter> where TWriter : IStreamingWriter<byte>
{
    StreamingWriter<TWriter> _writer;
    readonly FlushControl _flushControl;

    public MessageWriter(TWriter writer, FlushControl flushControl)
    {
        _writer = new StreamingWriter<TWriter>(writer);
        _flushControl = flushControl;
        AdvisoryFlushThreshold = _flushControl.FlushThreshold < AdvisoryFlushThreshold ? _flushControl.FlushThreshold : MessageWriter.DefaultAdvisoryFlushThreshold;
    }

    public int AdvisoryFlushThreshold { get; }

    public void WriteRaw(ReadOnlySpan<byte> value) => _writer.Write(value);
    public void WriteByte(byte value) => _writer.WriteByte(value);
    public void WriteUShort(ushort value) => _writer.WriteUShort(value);
    public void WriteShort(short value) => _writer.WriteShort(value);
    public void WriteInt(int value) => _writer.WriteInt(value);
    public void WriteCString(string value) => _writer.WriteCString(value);

    public long BufferedBytes => Writer.BufferedBytes;
    public long BytesCommitted => Writer.BytesCommitted;
    public long BytesPending => _flushControl.UnflushedBytes;

    public ref StreamingWriter<TWriter> Writer => ref _writer;

    public BufferWriter<TWriter> GetBufferWriter()
    {
        if (Writer.BufferedBytes > 0)
            throw new InvalidOperationException("Buffer writer cannot be created if the streaming writer has buffered bytes.");

        return BufferWriter<TWriter>.CreateFrom(Writer);
    }

    public void CommitBufferWriter(BufferWriter<TWriter> buffer) => Writer.CommitBufferWriter(buffer);

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

        _writer = new StreamingWriter<TWriter>(Writer.Output);
    }
}

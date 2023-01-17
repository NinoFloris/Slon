using System;
using System.Buffers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Slon.Buffers;

namespace Slon.Protocol;

static class MessageWriter
{
    public static int GetCStringByteCount(SizedString value, Encoding encoding)
    {
        if (value.ByteCount is null)
            value = value.WithEncoding(encoding);

        return value.ByteCount.GetValueOrDefault() + 1;
    }

    public static int GetCStringByteCount(string value, Encoding encoding)
        => value.Length != 0 ? encoding.GetByteCount(value) + 1 : 1;

    public const int IntByteCount = sizeof(int);
    public const int ShortByteCount = sizeof(short);
    public const int ByteByteCount = sizeof(byte);
    // About the default MTU payload size, not sure how much, if any, it helps.
    public const int DefaultAdvisoryFlushThreshold = 1450;
}

// This class exists purely to hold a StreamingWriter, as async methods can't have byrefs.
class MessageWriter<TWriter> where TWriter : IStreamingWriter<byte>
{
    StreamingWriter<TWriter> _writer;
    readonly FlushControl _flushControl;

    public MessageWriter(TWriter writer, FlushControl flushControl)
    {
        _writer = new StreamingWriter<TWriter>(writer);
        _flushControl = flushControl;
        AdvisoryFlushThreshold = Math.Min(_flushControl.FlushThreshold, MessageWriter.DefaultAdvisoryFlushThreshold);
    }

    public int AdvisoryFlushThreshold { get; }

    public void WriteRaw(ReadOnlySpan<byte> value) => _writer.Write(value);
    public void WriteByte(byte value) => _writer.WriteByte(value);
    public void WriteUShort(ushort value) => _writer.WriteUShort(value);
    public void WriteShort(short value) => _writer.WriteShort(value);
    public void WriteInt(int value) => _writer.WriteInt(value);
    public void WriteCString(string value, Encoding encoding) => _writer.WriteCString(value, encoding);
    public void WriteCString(SizedString value, Encoding encoding) => _writer.WriteCString(value, encoding);

    public long BufferedBytes => Writer.BufferedBytes;
    public long BytesCommitted => Writer.BytesCommitted;
    public long BytesPending => _flushControl.UnflushedBytes;

    public ref StreamingWriter<TWriter> Writer => ref _writer;

    public BufferWriter<TWriter> GetBufferWriter()
    {
        if (Writer.BufferedBytes > 0)
            ThrowBufferedBytesRemain();

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
            ThrowBufferedBytesRemain();

        _writer = new StreamingWriter<TWriter>(Writer.Output);
    }

    static void ThrowBufferedBytesRemain() => throw new InvalidOperationException("The writer has buffered bytes remaining.");
}

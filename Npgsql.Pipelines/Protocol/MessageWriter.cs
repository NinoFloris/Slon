using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
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
    public static int GetCStringByteCount(string value) =>
        value is "" ? 1 : PgEncoding.UTF8.GetByteCount(value.AsSpan()) + 1;
    public const int IntByteCount = sizeof(int);
    public const int ShortByteCount = sizeof(short);
    public const int ByteByteCount = sizeof(byte);
}

class MessageWriter<T> where T : IBufferWriter<byte>
{
    BufferWriter<T> _writer;
    readonly FlushControl? _flushControl;

    public MessageWriter(T writer)
    {
        _writer = new BufferWriter<T>(writer);
        // About the default MTU payload size.
        CommitThreshold = 1450;
    }

    public MessageWriter(T writer, FlushControl flushFlushControl)
        :this(writer)
    {
        _flushControl = flushFlushControl;
        if (_flushControl.BytesThreshold < CommitThreshold)
            CommitThreshold = _flushControl.BytesThreshold;
    }

    public int CommitThreshold { get; }

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

    public async ValueTask<FlushResult> WriteHugeCStringAsync(string value, CancellationToken cancellationToken = default)
    {
        var offset = 0;
        Encoder? encoder = null;
        var writer = _writer;
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

    public long BufferedBytes => _writer.BufferedBytes;
    public long BytesCommitted => _writer.BytesCommitted;

    [MemberNotNullWhen(true, "_flushControl")]
    public bool CanFlush => _flushControl is not null;

    ValueTask<FlushResult> FlushAsyncCore(bool observeFlushThreshold, CancellationToken cancellationToken)
    {
        if (!CanFlush)
            throw new NotSupportedException("This instance is not flushable.");

        Commit();
        return _flushControl.FlushAsync(observeFlushThreshold, cancellationToken);
    }

    /// Commit and Flush on the underlying writer if threshold is reached.
    /// Commit is always executed, independent of the flush threshold being reached.
    public ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default) => FlushAsyncCore(observeFlushThreshold: true, cancellationToken);

    /// Only to be used by the protocol (and even then flush control can still decide not to flush).
    /// Commit and Flush on the underlying writer if threshold is reached.
    /// Commit is always executed, independent of the flush threshold being reached.
    internal ValueTask<FlushResult> ForceFlushAsync(CancellationToken cancellationToken = default) => FlushAsyncCore(observeFlushThreshold: false, cancellationToken);

    public void Reset()
    {
        if (_writer.BufferedBytes > 0)
            throw new InvalidOperationException("Resetting writer while there are still buffered bytes.");

        _writer = new BufferWriter<T>(_writer.Output);
    }
}

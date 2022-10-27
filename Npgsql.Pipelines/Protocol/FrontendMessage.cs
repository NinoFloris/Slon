using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines.Protocol;

static class FrontendMessage {
    public static readonly bool DebugEnabled = false;

    class BufferedMessage: IFrontendMessage
    {
        readonly ICopyableBuffer<byte> _buffer;

        public BufferedMessage(ICopyableBuffer<byte> buffer) => _buffer = buffer;

        public bool CanWrite => true;
        public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
            => _buffer.CopyTo(buffer);
    }

    class StreamingMessage: IStreamingFrontendMessage
    {
        readonly Stream _stream;

        public StreamingMessage(Stream stream) => _stream = stream;

        public bool CanWrite => false;
        public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte> => throw new NotSupportedException();

        public async ValueTask<FlushResult> WriteAsync<T>(MessageWriter<T> writer, CancellationToken cancellationToken = default) where T : IBufferWriter<byte>
        {
            var read = 0;
            var flushResult = default(FlushResult);
            do
            {
                if (read > 7 * 1024)
                    writer.Writer.Ensure(8 * 1024);
                read = await _stream.ReadAsync(writer.Writer.Memory, cancellationToken);
                writer.Writer.Advance(read);
                if (read > writer.AdvisoryFlushThreshold)
                    flushResult = await writer.FlushAsync(cancellationToken);
            } while (read != 0);

            if (writer.UnflushedBytes != 0)
                flushResult = await writer.FlushAsync(cancellationToken);

            return flushResult;
        }
    }

    public static IFrontendMessage Create(Stream buffer) => new StreamingMessage(buffer);
    public static IFrontendMessage Create(ICopyableBuffer<byte> buffer) => new BufferedMessage(buffer);
}

interface IFrontendHeader<THeader> where THeader: struct, IFrontendHeader<THeader>
{
    /// <summary>
    /// Total message length, including any header bytes.
    /// </summary>
    public int Length { get; set;  }
    void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>;
}

interface IFrontendMessage
{
    // TODO bit of a weird api now
    bool CanWrite { get; }
    void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>;
}

interface IStreamingFrontendMessage: IFrontendMessage
{
    ValueTask<FlushResult> WriteAsync<T>(MessageWriter<T> writer, CancellationToken cancellationToken = default) where T : IBufferWriter<byte>;
}

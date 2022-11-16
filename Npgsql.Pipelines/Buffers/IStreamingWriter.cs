using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.Buffers;

// A streaming alternative to a System.IO.Stream, instead based on the preferable IBufferWriter.
interface IStreamingWriter<T>: IBufferWriter<T>
{
    // TODO maybe support unflushed bytes getter? Let's wait, might not be necessary.

    // TODO we may want to have Flush overloads/default params that take an advanceCount to reduce interface calls.
    void Flush(TimeSpan timeout = default);
    ValueTask FlushAsync(CancellationToken cancellationToken = default);
}

// Need to lift IBufferWriter implementations like Pipes to IStreamingWriter.
class PipeStreamingWriter: IStreamingWriter<byte>
{
    readonly PipeWriter _pipeWriter;
    public PipeStreamingWriter(PipeWriter pipeWriter) => _pipeWriter = pipeWriter;

    public void Advance(int count) => _pipeWriter.Advance(count);
    public Memory<byte> GetMemory(int sizeHint = 0) => _pipeWriter.GetMemory(sizeHint);
    public Span<byte> GetSpan(int sizeHint = 0) => _pipeWriter.GetSpan(sizeHint);

    public void Flush(TimeSpan timeout = default)
    {
        if (_pipeWriter is not ISyncCapablePipeWriter writer)
            throw new NotSupportedException("The underlying writer does not support sync operations.");

        // TODO handle flush results.
        var _ = writer.Flush(timeout);
    }

#if !NETSTANDARD2_0
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        // TODO handle flush results.
        var _ = await _pipeWriter.FlushAsync(cancellationToken);
    }
}


using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Pipelines;

interface IPipeReaderSyncSupport
{
    PipeReader PipeReader { get; }
    ReadResult Read(TimeSpan timeout = default);
}

interface IPipeWriterSyncSupport: IBufferWriter<byte>
{
    PipeWriter PipeWriter { get; }
    FlushResult Flush(TimeSpan timeout = default);
}

sealed class PipeWriterUnflushedBytes: PipeWriter
{
    readonly PipeWriter _pipeWriter;
    long _bytesBuffered;

    public PipeWriterUnflushedBytes(PipeWriter writer)
    {
        _pipeWriter = writer;
    }

    public override bool CanGetUnflushedBytes => true;
    public override long UnflushedBytes => _bytesBuffered;

    public override ValueTask CompleteAsync(Exception? exception = null) => _pipeWriter.CompleteAsync(exception);
    public override void Complete(Exception? exception = null) => _pipeWriter.Complete();

    public override void CancelPendingFlush() => _pipeWriter.CancelPendingFlush();

    public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
    {
        var result = await _pipeWriter.FlushAsync(cancellationToken);
        _bytesBuffered = 0;
        return result;
    }

    public override void Advance(int bytes)
    {
        _pipeWriter.Advance(bytes);
        _bytesBuffered += bytes;
    }

    public override Memory<byte> GetMemory(int sizeHint = 0) => _pipeWriter.GetMemory(sizeHint);
    public override Span<byte> GetSpan(int sizeHint = 0) => _pipeWriter.GetSpan(sizeHint);
}

class AsyncOnlyPipeReader : IPipeReaderSyncSupport
{
    public AsyncOnlyPipeReader(PipeReader reader)
    {
        PipeReader = reader;
    }

    public PipeReader PipeReader { get; }
    public ReadResult Read(TimeSpan timeout = default) => throw new NotSupportedException();
}

class AsyncOnlyPipeWriter : IPipeWriterSyncSupport
{
    public AsyncOnlyPipeWriter(PipeWriter writer)
    {
        PipeWriter = writer;
    }

    public PipeWriter PipeWriter { get; }
    public void Advance(int count) => PipeWriter.Advance(count);
    public Memory<byte> GetMemory(int sizeHint = 0) => PipeWriter.GetMemory(sizeHint);
    public Span<byte> GetSpan(int sizeHint = 0) => PipeWriter.GetSpan(sizeHint);
    public FlushResult Flush(TimeSpan timeout = default) => throw new NotSupportedException();
}

static class PipeReaderSyncSupportExtensions
{
    [DoesNotReturn]
    static void ThrowArgumentOutOfRangeException() => throw new ArgumentOutOfRangeException("minimumSize");

    public static ReadResult ReadAtLeast(this IPipeReaderSyncSupport reader, int minimumSize, TimeSpan timeout = default)
    {
        if (minimumSize < 0)
            ThrowArgumentOutOfRangeException();

        long start = -1;
        var timeoutMillis = Timeout.Infinite;
        if (timeout != TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan)
        {
            start = TickCount64Shim.Get();
            timeoutMillis = (int)timeout.TotalMilliseconds;
        }

        PipeReader? pipeReader = null;
        while (true)
        {
            var result = reader.Read(timeout);
            var buffer = result.Buffer;

            if (buffer.Length >= minimumSize || result.IsCompleted || result.IsCanceled)
                return result;

            // Keep buffering until we get more data
            pipeReader ??= reader.PipeReader;
            pipeReader.AdvanceTo(buffer.Start, buffer.End);
            if (start != -1)
            {
                var elapsed = TickCount64Shim.Get() - start;
                if (elapsed >= timeoutMillis)
                    throw new TimeoutException("The operation timed out.");
                timeout = TimeSpan.FromMilliseconds(timeoutMillis - elapsed);
            }
        }
    }
}

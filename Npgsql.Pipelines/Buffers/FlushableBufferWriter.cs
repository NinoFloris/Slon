using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.Buffers;

readonly struct FlushResult
{
    [Flags]
    enum ResultFlags
    {
        None = 0,
        Canceled = 1,
        Completed = 2
    }

    readonly ResultFlags _resultFlags;

    public FlushResult(bool isCanceled, bool isCompleted)
    {
        _resultFlags = ResultFlags.None;

        if (isCanceled)
        {
            _resultFlags |= ResultFlags.Canceled;
        }

        if (isCompleted)
        {
            _resultFlags |= ResultFlags.Completed;
        }
    }

    public bool IsCanceled => (_resultFlags & ResultFlags.Canceled) != 0;
    public bool IsCompleted => (_resultFlags & ResultFlags.Completed) != 0;
}

readonly struct CancellationTokenOrTimeout
{
    public TimeSpan Timeout { get; }
    public CancellationToken CancellationToken { get; }

    public bool IsTimeout => Timeout != TimeSpan.Zero;
    public bool IsCancellationToken => !IsTimeout;

    CancellationTokenOrTimeout(CancellationToken cancellationToken) => CancellationToken = cancellationToken;
    CancellationTokenOrTimeout(TimeSpan timeout) => Timeout = timeout;

    public static CancellationTokenOrTimeout CreateCancellationToken(CancellationToken cancellationToken) => new(cancellationToken);
    public static CancellationTokenOrTimeout CreateTimeout(TimeSpan timeout, bool zeroIsInfinite = true)
    {
        if (timeout == TimeSpan.Zero)
        {
            if (!zeroIsInfinite)
                throw new ArgumentOutOfRangeException(nameof(timeout), "Cannot be TimeSpan.Zero.");
            timeout = System.Threading.Timeout.InfiniteTimeSpan;
        }

        return new( timeout);
    }
}

interface IFlushableBufferWriter<T> : IBufferWriter<T>
{
    public ValueTask<FlushResult> FlushAsync(CancellationTokenOrTimeout cancellationToken = default);
}

class FlushableBufferWriter<T> : IFlushableBufferWriter<byte> where T: IBufferWriter<byte>
{
    public T Writer { get; }
    readonly Func<T, CancellationTokenOrTimeout, ValueTask<FlushResult>>? _flushAsync;

    public FlushableBufferWriter(T writer, Func<T, CancellationTokenOrTimeout, ValueTask<FlushResult>>? flushAsync)
    {
        Writer = writer;
        _flushAsync = flushAsync;
    }

    public void Advance(int count) => Writer.Advance(count);
    public Memory<byte> GetMemory(int sizeHint = 0) => Writer.GetMemory(sizeHint);
    public Span<byte> GetSpan(int sizeHint = 0) => Writer.GetSpan(sizeHint);

    // Make data available to underlying sinks.
    public ValueTask<FlushResult> FlushAsync(CancellationTokenOrTimeout cancellationToken = default) =>
        _flushAsync?.Invoke(Writer, cancellationToken) ?? new ValueTask<FlushResult>(new FlushResult());
}

static class FlushableBufferWriter
{
    static Func<PipeWriter, CancellationTokenOrTimeout, ValueTask<FlushResult>> PipeWriterFlushAsync { get; } =
        async (writer, cancellationToken) =>
        {
            var res = await writer.FlushAsync(cancellationToken.CancellationToken);
            return new FlushResult(isCanceled: res.IsCanceled, isCompleted: res.IsCompleted);
        };

    static Func<IPipeWriterSyncSupport, CancellationTokenOrTimeout, ValueTask<FlushResult>> PipeWriterFlush { get; } =
        (writer, cancellationToken) =>
        {
            var res = writer.Flush(cancellationToken.Timeout);
            return new ValueTask<FlushResult>(new FlushResult(isCanceled: res.IsCanceled, isCompleted: res.IsCompleted));
        };

    public static FlushableBufferWriter<PipeWriter> Create(PipeWriter pipeWriter) => new(pipeWriter, PipeWriterFlushAsync);
    public static FlushableBufferWriter<IPipeWriterSyncSupport> Create(IPipeWriterSyncSupport writerSyncSupport) => new(writerSyncSupport, PipeWriterFlush);
    public static FlushableBufferWriter<T> Create<T>(T writer) where T : IBufferWriter<byte> => new(writer, null);
}

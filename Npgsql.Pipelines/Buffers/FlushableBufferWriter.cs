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

interface IFlushableBufferWriter<T> : IBufferWriter<T>
{
    public ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default);
}

readonly struct FlushableBufferWriter<T> : IFlushableBufferWriter<byte> where T: IBufferWriter<byte>
{
    public T Writer { get; }
    readonly Func<T, CancellationToken, ValueTask<FlushResult>>? _flushAsync;

    public FlushableBufferWriter(T writer, Func<T, CancellationToken, ValueTask<FlushResult>>? flushAsync)
    {
        Writer = writer;
        _flushAsync = flushAsync;
    }

    public void Advance(int count) => Writer.Advance(count);
    public Memory<byte> GetMemory(int sizeHint = 0) => Writer.GetMemory(sizeHint);
    public Span<byte> GetSpan(int sizeHint = 0) => Writer.GetSpan(sizeHint);

    // Make data available to underlying sinks.
    public ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default) =>
        _flushAsync?.Invoke(Writer, cancellationToken) ?? new ValueTask<FlushResult>(new FlushResult());
}

static class FlushableBufferWriter
{
    static Func<PipeWriter, CancellationToken, ValueTask<FlushResult>> PipeWriterFlushAsync { get; } =
        async (writer, cancellationToken) =>
        {
            var res = await writer.FlushAsync(cancellationToken);
            return new FlushResult(isCanceled: res.IsCanceled, isCompleted: res.IsCompleted);
        };

    public static FlushableBufferWriter<PipeWriter> Create(PipeWriter writer) => new(writer, PipeWriterFlushAsync);
    public static FlushableBufferWriter<T> Create<T>(T writer) where T : IBufferWriter<byte> => new(writer, null);
}

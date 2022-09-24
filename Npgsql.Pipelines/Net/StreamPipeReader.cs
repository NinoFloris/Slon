using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines;

namespace System.IO.Pipelines;

class StreamPipeReader: PipeReader, IPipeReaderSyncSupport
{
    readonly Stream _stream;
    readonly bool _canTimeout;
    readonly int? _readTimeout;
    int _reading;

    public StreamPipeReader(Stream stream, StreamPipeReaderOptions options)
    {
        PipeReader = Create(stream, options);
        _stream = stream;
        _canTimeout = stream.CanTimeout;
        // Reading this is somewhat expensive so we cache it if leave open is false, as it conveys some amount of ownership (admittedly it's not perfect).
        _readTimeout = _canTimeout && !options.LeaveOpen ? _stream.ReadTimeout : null;
    }

    public PipeReader PipeReader {get; }

    public ReadResult Read(TimeSpan timeout = default)
    {
        int read;
        var timeoutMillis = Timeout.Infinite;
        var previousTimeout = timeoutMillis;
        var start = -1;
        try
        {
            // To map conceptually to pipelines, only one read can be active.
            if (Interlocked.CompareExchange(ref _reading, 1, 0) != 0)
                ThrowHelper.ThrowInvalidOperationException_AlreadyReading();

            if (_canTimeout)
            {
                if (timeout != Timeout.InfiniteTimeSpan)
                    timeoutMillis = (int)timeout.TotalMilliseconds;
                previousTimeout = _readTimeout ?? _stream.ReadTimeout;
                if (timeoutMillis != previousTimeout && timeoutMillis != 0 && timeoutMillis != Timeout.Infinite)
                    _stream.ReadTimeout = timeoutMillis;
                start = Environment.TickCount;
            }

            try
            {
                // TODO test whether zero byte reads actually work on netfx (test at least ssl and network stream).
#if NETSTANDARD2_0
                read = _stream.Read(Array.Empty<byte>(), 0, 0);
#else
                read = _stream.Read(Span<byte>.Empty);
#endif
            }
            catch (Exception ex) when (ex is ObjectDisposedException || (ex is IOException ioEx && ioEx.InnerException is ObjectDisposedException))
            {
                return new(buffer: default, isCompleted: true, isCanceled: false);
            }
            catch (IOException)
            {
                // We'll assume that if we're past our deadline a timeout was the reason for this exception, it sucks indeed.
                // Stream has no contract to communicate an IOException was specifically because of a read/write/close timeout.
                // This either means baking in all the different patterns (IOException wrapping SocketException etc.), or doing this.
                if (start != -1 && start + timeoutMillis - start < 1)
                    return new (buffer: default, isCompleted: false, isCanceled: true);
                throw;
            }
        }
        finally
        {
            if (start != -1)
                _stream.ReadTimeout = previousTimeout;
            Interlocked.Exchange(ref _reading, 0);
        }

        if (read == -1)
        {
            PipeReader.Complete();
            return new(buffer: default, isCompleted: true, isCanceled: false);
        }

        var task = PipeReader.ReadAsync();
        // This would be a faulty stream pipe reader implementation.
        if (!task.IsCompleted)
            ThrowHelper.ThrowInvalidOperationException_InvalidReadAsync();

        return task.GetAwaiter().GetResult();
    }

    public override bool TryRead(out ReadResult result) => PipeReader.TryRead(out result);

    public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default) => PipeReader.ReadAsync(cancellationToken);

    public override void AdvanceTo(SequencePosition consumed) => PipeReader.AdvanceTo(consumed);

    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined) => PipeReader.AdvanceTo(consumed, examined);

    public override void CancelPendingRead() => PipeReader.CancelPendingRead();

    public override void Complete(Exception? exception = null) => PipeReader.Complete(exception);
}

using System;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.Buffers;

abstract class FlushControl: IDisposable
{
    public abstract TimeSpan FlushTimeout { get; }
    public abstract int BytesThreshold { get; }
    public abstract CancellationToken TimeoutCancellationToken { get; }
    public abstract ValueTask<FlushResult> FlushAsync(bool observeFlushThreshold = true, CancellationToken cancellationToken = default); 
    protected bool _disposed;

    public static FlushControl Create(IPipeWriterSyncSupport writer, TimeSpan flushTimeout, int flushThreshold, CancellationTokenOrTimeout cancellationToken = default)
    {
        if (!writer.PipeWriter.CanGetUnflushedBytes)
            throw new ArgumentException("Cannot accept PipeWriters that don't support UnflushedBytes.", nameof(writer));
        var flushControl = new ResettableFlushControl(writer, flushTimeout, flushThreshold);
        flushControl.Initialize(cancellationToken);
        return flushControl;
    }

    protected virtual void Dispose(bool disposing)
    {
    }

    public void Dispose()
    {
        _disposed = true;
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException("FlushControl");
    }
}

class ResettableFlushControl: FlushControl
{
    readonly IPipeWriterSyncSupport _writer;
    readonly PipeWriter _pipeWriter;
    CancellationTokenOrTimeout _cancellationToken;
    CancellationTokenSource? _timeoutSource;
    CancellationTokenRegistration? _registration;
    long _start = -1;

    public ResettableFlushControl(IPipeWriterSyncSupport writer, TimeSpan flushTimeout, int flushThreshold)
    {
        _writer = writer;
        _pipeWriter = writer.PipeWriter;
        FlushTimeout = flushTimeout;
        BytesThreshold = flushThreshold;
    }

    public override TimeSpan FlushTimeout { get; }
    public override int BytesThreshold { get; }
    public override CancellationToken TimeoutCancellationToken => _timeoutSource?.Token ?? CancellationToken.None;

    TimeSpan GetTimeout()
    {
        ThrowIfDisposed();
        if (_start != -1)
        {
            var remaining = _cancellationToken.Timeout - TimeSpan.FromMilliseconds(TickCount64Shim.Get() - _start);
            if (remaining <= TimeSpan.Zero)
                throw new TimeoutException();

            return remaining < FlushTimeout ? remaining : FlushTimeout;
        }

        return FlushTimeout;
    }

    CancellationToken GetToken()
    {
        ThrowIfDisposed();
        _timeoutSource!.Token.ThrowIfCancellationRequested();
        _timeoutSource.CancelAfter(FlushTimeout);
        return _timeoutSource.Token;
    }

    void CompleteFlush()
    {
        _timeoutSource?.CancelAfter(Timeout.Infinite);
    }

    bool AlwaysObserveFlushThreshold { get; set; } = false;

    public override ValueTask<FlushResult> FlushAsync(bool observeFlushThreshold = true, CancellationToken cancellationToken = default)
    {
        if (AlwaysObserveFlushThreshold || observeFlushThreshold)
        {
            if (BytesThreshold != -1 && BytesThreshold > _pipeWriter.UnflushedBytes)
                return new ValueTask<FlushResult>(new FlushResult(isCanceled: false, isCompleted: false));
        }

        return FlushAsyncCore();

        async ValueTask<FlushResult> FlushAsyncCore()
        {
            try
            {
                System.IO.Pipelines.FlushResult result;
                if (_timeoutSource is null)
                {
                    result = _writer.Flush(GetTimeout());
                }
                else
                {
                    result = await _writer.PipeWriter.FlushAsync(GetToken());
                }

                return new FlushResult(isCanceled: result.IsCanceled, isCompleted: result.IsCompleted);
            }
            finally
            {
                CompleteFlush();
            }
        }
    }

    internal void Initialize(TimeSpan timeout) => Initialize(CancellationTokenOrTimeout.CreateTimeout(timeout));
    internal void Initialize(CancellationToken cancellationToken) => Initialize(CancellationTokenOrTimeout.CreateCancellationToken(cancellationToken));
    internal void Initialize(CancellationTokenOrTimeout cancellationToken)
    {
        ThrowIfDisposed();
        if (_registration is not null)
            throw new InvalidOperationException("Initialize called before Reset, concurrent use is not supported.");

        _cancellationToken = cancellationToken;
        if (cancellationToken.IsCancellationToken)
        {
            _timeoutSource ??= new CancellationTokenSource();
            _registration = _cancellationToken.CancellationToken.UnsafeRegister(state => ((CancellationTokenSource)state!).Cancel(), _timeoutSource);
        }
        else
        {
            _cancellationToken = cancellationToken = CancellationTokenOrTimeout.CreateTimeout(TimeSpan.FromMilliseconds(1000));
            _start = cancellationToken.Timeout == Timeout.InfiniteTimeSpan ? -1 : TickCount64Shim.Get();
        }
    }

    internal void Reset()
    {
        ThrowIfDisposed();
        _start = -1;
        if (_timeoutSource is not null)
        {
            _registration?.Dispose();
            _registration = null;
#if NETSTANDARD2_0
            // Best effort, as we can't actually reset the registrations (sockets do correctly unregister when they complete).
            // Npgsql would have the same issues otherwise.
            _timeoutSource.CancelAfter(Timeout.Infinite);
            if (_timeoutSource.IsCancellationRequested)
            {
                _timeoutSource.Dispose();
                _timeoutSource = null;
            }
#else
            if (!_timeoutSource.TryReset())
            {
                _timeoutSource.Dispose();
                _timeoutSource = null;
            }
#endif
        }
    }

    protected override void Dispose(bool disposing)
    {
        _registration?.Dispose();
        _timeoutSource?.Dispose();
    }
}

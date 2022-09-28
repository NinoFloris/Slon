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
    public abstract bool IsFlushBlocking { get; }
    public abstract long UnflushedBytes { get; }
    public abstract ValueTask<FlushResult> FlushAsync(bool observeFlushThreshold = true, CancellationToken cancellationToken = default);
    protected bool _disposed;

    public static FlushControl Create(IPipeWriterSyncSupport writer, TimeSpan flushTimeout, int flushThreshold)
    {
        if (!writer.PipeWriter.CanGetUnflushedBytes)
            throw new ArgumentException("Cannot accept PipeWriters that don't support UnflushedBytes.", nameof(writer));
        var flushControl = new ResettableFlushControl(writer, flushTimeout, flushThreshold);
        flushControl.Initialize();
        return flushControl;
    }

    public static FlushControl Create(IPipeWriterSyncSupport writer, TimeSpan flushTimeout, int flushThreshold, TimeSpan userTimeout)
    {
        if (!writer.PipeWriter.CanGetUnflushedBytes)
            throw new ArgumentException("Cannot accept PipeWriters that don't support UnflushedBytes.", nameof(writer));
        var flushControl = new ResettableFlushControl(writer, flushTimeout, flushThreshold);
        flushControl.Initialize(userTimeout);
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
    TimeSpan _userTimeout;
    CancellationTokenSource? _timeoutSource;
    CancellationTokenRegistration? _registration;
    long _start = -2;

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
    public override bool IsFlushBlocking => _timeoutSource is null;
    public override long UnflushedBytes => _pipeWriter.UnflushedBytes;

    TimeSpan GetTimeout()
    {
        ThrowIfDisposed();
        if (_start != -1)
        {
            var remaining = _userTimeout - TimeSpan.FromMilliseconds(TickCount64Shim.Get() - _start);
            if (remaining <= TimeSpan.Zero)
                throw new TimeoutException();

            return remaining < FlushTimeout ? remaining : FlushTimeout;
        }

        return FlushTimeout;
    }

    CancellationToken GetToken(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        _timeoutSource!.CancelAfter(FlushTimeout);
        _timeoutSource.Token.ThrowIfCancellationRequested();
        _registration = cancellationToken.UnsafeRegister(state => ((CancellationTokenSource)state!).Cancel(), _timeoutSource);
        return _timeoutSource.Token;
    }

    void CompleteFlush()
    {
        _timeoutSource?.CancelAfter(Timeout.Infinite);
        _registration?.Dispose();
    }

    public bool AlwaysObserveFlushThreshold { get; set; } = false;

    public override ValueTask<FlushResult> FlushAsync(bool observeFlushThreshold = true, CancellationToken cancellationToken = default)
    {
        if (AlwaysObserveFlushThreshold || observeFlushThreshold)
        {
            if (BytesThreshold != -1 && BytesThreshold > _pipeWriter.UnflushedBytes)
                return new ValueTask<FlushResult>(new FlushResult(isCanceled: false, isCompleted: false, isIgnored: true));
        }

        if (_pipeWriter.UnflushedBytes == 0)
            return new ValueTask<FlushResult>(new FlushResult(isCanceled: false, isCompleted: false, isIgnored: true));

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
                    try
                    {
                        result = await _writer.PipeWriter.FlushAsync(GetToken(cancellationToken));
                    }
                    catch (OperationCanceledException ex) when (TimeoutCancellationToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                    {
                        throw new TimeoutException("The operation has timed out.", ex);
                    }
                }

                return new FlushResult(isCanceled: result.IsCanceled, isCompleted: result.IsCompleted, isIgnored: false);
            }
            finally
            {
                CompleteFlush();
            }
        }
    }

    internal void Initialize(TimeSpan timeout)
    {
        ThrowIfDisposed();
        if (_start != -2)
            throw new InvalidOperationException("Initialize called before Reset, concurrent use is not supported.");

        _start = _userTimeout.Ticks <= 0 ? -1 : TickCount64Shim.Get();
        _userTimeout = timeout;
    }

    internal void Initialize()
    {
        ThrowIfDisposed();
        if (_start != -2)
            throw new InvalidOperationException("Initialize called before Reset, concurrent use is not supported.");

        _start = -1;
        _timeoutSource ??= new CancellationTokenSource();
    }

    internal void Reset()
    {
        ThrowIfDisposed();
        _start = -2;
        AlwaysObserveFlushThreshold = false;
        if (_timeoutSource is not null)
        {
            _registration?.Dispose();
            _registration = null;
            if (!_timeoutSource.TryReset())
            {
                _timeoutSource.Dispose();
                _timeoutSource = null;
            }
        }
    }

    protected override void Dispose(bool disposing)
    {
        _registration?.Dispose();
        _timeoutSource?.Dispose();
    }
}

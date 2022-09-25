using System;
using System.Threading;

namespace Npgsql.Pipelines.Buffers;

abstract class FlushControl: IDisposable
{
    public abstract TimeSpan FlushTimeout { get; }
    public abstract int BytesThreshold { get; }
    public abstract CancellationToken TimeoutCancellationToken { get; }
    internal abstract CancellationTokenOrTimeout GetFlushToken();
    internal abstract void CompleteFlush();
    protected bool _disposed;

    public static FlushControl Create(TimeSpan flushTimeout, int flushThreshold, CancellationTokenOrTimeout cancellationToken = default)
    {
        var flushControl = new ResettableFlushControl(flushTimeout, flushThreshold);
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
    CancellationTokenOrTimeout _cancellationToken;
    CancellationTokenSource? _timeoutSource;
    CancellationTokenRegistration? _registration;
    long _start = -1;

    public ResettableFlushControl(TimeSpan flushTimeout, int flushThreshold)
    {
        FlushTimeout = flushTimeout;
        BytesThreshold = flushThreshold;
    }

    public override TimeSpan FlushTimeout { get; }
    public override int BytesThreshold { get; }
    public override CancellationToken TimeoutCancellationToken => _timeoutSource?.Token ?? CancellationToken.None;

    internal override CancellationTokenOrTimeout GetFlushToken()
    {
        ThrowIfDisposed();
        if (_cancellationToken.IsCancellationToken)
        {
            _timeoutSource!.Token.ThrowIfCancellationRequested();
            _timeoutSource.CancelAfter(FlushTimeout);
            return CancellationTokenOrTimeout.CreateCancellationToken(_timeoutSource.Token);
        }
        else
        {
            if (_start != -1)
            {
                var remaining = _cancellationToken.Timeout - TimeSpan.FromMilliseconds(TickCount64Shim.Get() - _start);
                if (remaining <= TimeSpan.Zero)
                    throw new TimeoutException();

                return CancellationTokenOrTimeout.CreateTimeout(remaining < FlushTimeout ? remaining : FlushTimeout);
            }

            return CancellationTokenOrTimeout.CreateTimeout(FlushTimeout);
        }
    }

    internal override void CompleteFlush()
    {
        _timeoutSource?.CancelAfter(Timeout.Infinite);
    }

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

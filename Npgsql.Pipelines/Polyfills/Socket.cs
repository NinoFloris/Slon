using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace System.Net.Sockets;

#if NETSTANDARD2_0

static class SocketExtensions
{
    /// <summary>
    /// Awaitable SocketAsyncEventArgs, where awaiting the args yields either the BytesTransferred or throws the relevant socket exception
    /// </summary>
    class SocketAwaitableEventArgs : SocketAsyncEventArgs, ICriticalNotifyCompletion
    {
        static readonly Action? _callbackCompleted = () => { };

        readonly PipeScheduler? _ioScheduler;

        Action? _callback;

        static readonly Action<object?> InvokeStateAsAction = state => ((Action)state!)();

        /// <summary>
        /// Create a new SocketAwaitableEventArgs instance, optionally providing a scheduler for callbacks
        /// </summary>
        /// <param name="ioScheduler"></param>
        public SocketAwaitableEventArgs(PipeScheduler? ioScheduler = null)
        {
            // treat null and Inline interchangeably
            if (ReferenceEquals(ioScheduler, PipeScheduler.Inline)) ioScheduler = null;
            _ioScheduler = ioScheduler;
        }

        /// <summary>
        /// Get the awaiter for this instance; used as part of "await"
        /// </summary>
        public SocketAwaitableEventArgs GetAwaiter() => this;

        /// <summary>
        /// Indicates whether the current operation is complete; used as part of "await"
        /// </summary>
        public bool IsCompleted => ReferenceEquals(_callback, _callbackCompleted);

        /// <summary>
        /// Gets the result of the async operation is complete; used as part of "await"
        /// </summary>
        public int GetResult()
        {
            Debug.Assert(ReferenceEquals(_callback, _callbackCompleted));

            _callback = null;

            if (SocketError != SocketError.Success)
            {
                ThrowSocketException(SocketError);
            }

            return BytesTransferred;

            static void ThrowSocketException(SocketError e)
            {
                throw new SocketException((int)e);
            }
        }

        /// <summary>
        /// Schedules a continuation for this operation; used as part of "await"
        /// </summary>
        public void OnCompleted(Action continuation)
        {
            if (ReferenceEquals(Volatile.Read(ref _callback), _callbackCompleted)
                || ReferenceEquals(Interlocked.CompareExchange(ref _callback, continuation, null), _callbackCompleted))
            {
                // this is the rare "kinda already complete" case; push to worker to prevent possible stack dive,
                // but prefer the custom scheduler when possible
                if (_ioScheduler == null)
                {
                    Task.Run(continuation);
                }
                else
                {
                    _ioScheduler.Schedule(InvokeStateAsAction, continuation);
                }
            }
        }

        /// <summary>
        /// Schedules a continuation for this operation; used as part of "await"
        /// </summary>
        public void UnsafeOnCompleted(Action continuation)
        {
            OnCompleted(continuation);
        }

        /// <summary>
        /// Marks the operation as complete - this should be invoked whenever a SocketAsyncEventArgs operation returns false
        /// </summary>
        public void Complete()
        {
            OnCompleted(this);
        }

        /// <summary>
        /// Invoked automatically when an operation completes asynchronously
        /// </summary>
        protected override void OnCompleted(SocketAsyncEventArgs e)
        {
            var continuation = Interlocked.Exchange(ref _callback, _callbackCompleted);

            if (continuation != null)
            {
                if (_ioScheduler == null)
                {
                    continuation.Invoke();
                }
                else
                {
                    _ioScheduler.Schedule(InvokeStateAsAction, continuation);
                }
            }
        }
    }

    public static async ValueTask ConnectAsync(this Socket socket, EndPoint remoteEndPoint, CancellationToken cancellationToken = default)
    {
        using var args = new SocketAwaitableEventArgs();
        args.RemoteEndPoint = remoteEndPoint;
        if (!socket.ConnectAsync(args)) args.Complete();
        await args;
    }
}

#endif

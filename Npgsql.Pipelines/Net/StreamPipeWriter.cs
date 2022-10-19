// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines;

namespace System.IO.Pipelines
{
    internal sealed class BufferSegment : ReadOnlySequenceSegment<byte>
    {
        private IMemoryOwner<byte>? _memoryOwner;
        private byte[]? _array;
        private BufferSegment? _next;
        private int _end;

        /// <summary>
        /// The End represents the offset into AvailableMemory where the range of "active" bytes ends. At the point when the block is leased
        /// the End is guaranteed to be equal to Start. The value of Start may be assigned anywhere between 0 and
        /// Buffer.Length, and must be equal to or less than End.
        /// </summary>
        public int End
        {
            get => _end;
            set
            {
                Debug.Assert(value <= AvailableMemory.Length);

                _end = value;
                Memory = AvailableMemory.Slice(0, value);
            }
        }

        /// <summary>
        /// Reference to the next block of data when the overall "active" bytes spans multiple blocks. At the point when the block is
        /// leased Next is guaranteed to be null. Start, End, and Next are used together in order to create a linked-list of discontiguous
        /// working memory. The "active" memory is grown when bytes are copied in, End is increased, and Next is assigned. The "active"
        /// memory is shrunk when bytes are consumed, Start is increased, and blocks are returned to the pool.
        /// </summary>
        public BufferSegment? NextSegment
        {
            get => _next;
            set
            {
                Next = value;
                _next = value;
            }
        }

        public void SetOwnedMemory(IMemoryOwner<byte> memoryOwner)
        {
            _memoryOwner = memoryOwner;
            AvailableMemory = memoryOwner.Memory;
        }

        public void SetOwnedMemory(byte[] arrayPoolBuffer)
        {
            _array = arrayPoolBuffer;
            AvailableMemory = arrayPoolBuffer;
        }

        // Resets memory and internal state, should be called when removing the segment from the linked list
        public void Reset()
        {
            ResetMemory();

            Next = null;
            RunningIndex = 0;
            _next = null;
        }

        // Resets memory only, should be called when keeping the BufferSegment in the linked list and only swapping out the memory
        public void ResetMemory()
        {
            IMemoryOwner<byte>? memoryOwner = _memoryOwner;
            if (memoryOwner != null)
            {
                _memoryOwner = null;
                memoryOwner.Dispose();
            }
            else
            {
                Debug.Assert(_array != null);
                ArrayPool<byte>.Shared.Return(_array);
                _array = null;
            }


            Memory = default;
            _end = 0;
            AvailableMemory = default;
        }

        // Exposed for testing
        internal object? MemoryOwner => (object?)_memoryOwner ?? _array;

        public Memory<byte> AvailableMemory { get; private set; }

        public int Length => End;

        public int WritableBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => AvailableMemory.Length - End;
        }

        public void SetNext(BufferSegment segment)
        {
            Debug.Assert(segment != null);
            Debug.Assert(Next == null);

            NextSegment = segment;

            segment = this;

            while (segment.Next != null)
            {
                Debug.Assert(segment.NextSegment != null);
                segment.NextSegment!.RunningIndex = segment.RunningIndex + segment.Length;
                segment = segment.NextSegment;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetLength(BufferSegment startSegment, int startIndex, BufferSegment endSegment, int endIndex)
        {
            return (endSegment.RunningIndex + (uint)endIndex) - (startSegment.RunningIndex + (uint)startIndex);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetLength(long startPosition, BufferSegment endSegment, int endIndex)
        {
            return (endSegment.RunningIndex + (uint)endIndex) - startPosition;
        }
    }

    internal struct BufferSegmentStack
    {
        private SegmentAsValueType[] _array;
        private int _size;

        public BufferSegmentStack(int size)
        {
            _array = new SegmentAsValueType[size];
            _size = 0;
        }

        public int Count => _size;

        public bool TryPop([NotNullWhen(true)] out BufferSegment? result)
        {
            int size = _size - 1;
            SegmentAsValueType[] array = _array;

            if ((uint)size >= (uint)array.Length)
            {
                result = default;
                return false;
            }

            _size = size;
            result = array[size];
            array[size] = default;
            return true;
        }

        // Pushes an item to the top of the stack.
        public void Push(BufferSegment item)
        {
            int size = _size;
            SegmentAsValueType[] array = _array;

            if ((uint)size < (uint)array.Length)
            {
                array[size] = item;
                _size = size + 1;
            }
            else
            {
                PushWithResize(item);
            }
        }

        // Non-inline from Stack.Push to improve its code quality as uncommon path
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void PushWithResize(BufferSegment item)
        {
            Array.Resize(ref _array, 2 * _array.Length);
            _array[_size] = item;
            _size++;
        }

        /// <summary>
        /// A simple struct we wrap reference types inside when storing in arrays to
        /// bypass the CLR's covariant checks when writing to arrays.
        /// </summary>
        /// <remarks>
        /// We use <see cref="SegmentAsValueType"/> as a wrapper to avoid paying the cost of covariant checks whenever
        /// the underlying array that the <see cref="BufferSegmentStack"/> class uses is written to.
        /// We've recognized this as a perf win in ETL traces for these stack frames:
        /// clr!JIT_Stelem_Ref
        ///   clr!ArrayStoreCheck
        ///     clr!ObjIsInstanceOf
        /// </remarks>
        private readonly struct SegmentAsValueType
        {
            private readonly BufferSegment _value;
            private SegmentAsValueType(BufferSegment value) => _value = value;
            public static implicit operator SegmentAsValueType(BufferSegment s) => new SegmentAsValueType(s);
            public static implicit operator BufferSegment(SegmentAsValueType s) => s._value;
        }
    }

    internal static class ThrowHelper
    {
        [DoesNotReturn]
        internal static void ThrowArgumentOutOfRangeException(ExceptionArgument argument) => throw CreateArgumentOutOfRangeException(argument);
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static Exception CreateArgumentOutOfRangeException(ExceptionArgument argument) => new ArgumentOutOfRangeException(argument.ToString());

        [DoesNotReturn]
        internal static void ThrowArgumentNullException(ExceptionArgument argument) => throw CreateArgumentNullException(argument);
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static Exception CreateArgumentNullException(ExceptionArgument argument) => new ArgumentNullException(argument.ToString());

        [DoesNotReturn]
        public static void ThrowInvalidOperationException_NoWritingAllowed() => throw CreateInvalidOperationException_NoWritingAllowed();
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static Exception CreateInvalidOperationException_NoWritingAllowed() => new InvalidOperationException("Writing is not allowed after writer was completed.");

        [DoesNotReturn]
        public static void ThrowInvalidOperationException_AlreadyFlushing() => throw CreateInvalidOperationException_AlreadyFlushing();
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static Exception CreateInvalidOperationException_AlreadyFlushing() => new InvalidOperationException("Concurrent flushes are not supported.");

        [DoesNotReturn]
        public static void ThrowInvalidOperationException_AlreadyReading() => throw CreateInvalidOperationException_AlreadyReading();
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static Exception CreateInvalidOperationException_AlreadyReading() => new InvalidOperationException("Concurrent reads are not supported.");

        [DoesNotReturn]
        public static void ThrowInvalidOperationException_InvalidReadAsync() => throw CreateInvalidOperationException_InvalidReadAsync();
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static Exception CreateInvalidOperationException_InvalidReadAsync() => new InvalidOperationException("The PipeReader still went async after its underlying stream completed a zero byte read.");

    }

    internal enum ExceptionArgument
    {
        minimumSize,
        bytes,
        callback,
        options,
        pauseWriterThreshold,
        resumeWriterThreshold,
        sizeHint,
        destination,
        buffer,
        source,
        readingStream,
        writingStream,
    }

    internal sealed class StreamPipeWriter : PipeWriter, IPipeWriterSyncSupport
    {
        internal const int InitialSegmentPoolSize = 4; // 16K
        internal const int MaxSegmentPoolSize = 256; // 1MB

        private readonly int _minimumBufferSize;

        private BufferSegment? _head;
        private BufferSegment? _tail;
        private Memory<byte> _tailMemory;
        private int _tailBytesBuffered;
        private int _bytesBuffered;

        private readonly MemoryPool<byte>? _pool;
        private readonly int _maxPooledBufferSize;

        private CancellationTokenSource? _internalTokenSource;
        private bool _isCompleted;
        private readonly object _lockObject = new object();

        private BufferSegmentStack _bufferSegmentPool;
        private readonly bool _leaveOpen;
        private readonly bool _canTimeout;
        private readonly int? _writeTimeout;

        private CancellationTokenSource InternalTokenSource
        {
            get
            {
                lock (_lockObject)
                {
                    return _internalTokenSource ??= new CancellationTokenSource();
                }
            }
        }

        public StreamPipeWriter(Stream writingStream, StreamPipeWriterOptions options)
        {
            if (writingStream is null)
            {
                ThrowHelper.ThrowArgumentNullException(ExceptionArgument.writingStream);
            }
            if (options is null)
            {
                ThrowHelper.ThrowArgumentNullException(ExceptionArgument.options);
            }

            InnerStream = writingStream;
            _minimumBufferSize = options.MinimumBufferSize;
            _pool = options.Pool == MemoryPool<byte>.Shared ? null : options.Pool;
            _maxPooledBufferSize = _pool?.MaxBufferSize ?? -1;
            _bufferSegmentPool = new BufferSegmentStack(InitialSegmentPoolSize);
            _leaveOpen = options.LeaveOpen;
            _canTimeout = writingStream.CanTimeout;
            // Reading this is somewhat expensive so we cache it if leave open is false, as it conveys some amount of ownership (admittedly it's not perfect).
            _writeTimeout = _canTimeout && !_leaveOpen ? writingStream.WriteTimeout : null;
        }

        /// <summary>
        /// Gets the inner stream that is being written to.
        /// </summary>
        public Stream InnerStream { get; }

        /// <inheritdoc />
        public override void Advance(int bytes)
        {
            if ((uint)bytes > (uint)_tailMemory.Length)
            {
                ThrowHelper.ThrowArgumentOutOfRangeException(ExceptionArgument.bytes);
            }

            _tailBytesBuffered += bytes;
            _bytesBuffered += bytes;
            _tailMemory = _tailMemory.Slice(bytes);
        }

        /// <inheritdoc />
        public override Memory<byte> GetMemory(int sizeHint = 0)
        {
            if (_isCompleted)
            {
                ThrowHelper.ThrowInvalidOperationException_NoWritingAllowed();
            }

            if (sizeHint < 0)
            {
                ThrowHelper.ThrowArgumentOutOfRangeException(ExceptionArgument.sizeHint);
            }

            AllocateMemory(sizeHint);

            return _tailMemory;
        }

        /// <inheritdoc />
        public override Span<byte> GetSpan(int sizeHint = 0)
        {
            if (_isCompleted)
            {
                ThrowHelper.ThrowInvalidOperationException_NoWritingAllowed();
            }

            if (sizeHint < 0)
            {
                ThrowHelper.ThrowArgumentOutOfRangeException(ExceptionArgument.sizeHint);
            }

            AllocateMemory(sizeHint);

            return _tailMemory.Span;
        }

        private void AllocateMemory(int sizeHint)
        {
            if (_head == null)
            {
                // We need to allocate memory to write since nobody has written before
                BufferSegment newSegment = AllocateSegment(sizeHint);

                // Set all the pointers
                _head = _tail = newSegment;
                _tailBytesBuffered = 0;
            }
            else
            {
                Debug.Assert(_tail != null);
                int bytesLeftInBuffer = _tailMemory.Length;

                if (bytesLeftInBuffer == 0 || bytesLeftInBuffer < sizeHint)
                {
                    if (_tailBytesBuffered > 0)
                    {
                        // Flush buffered data to the segment
                        _tail!.End += _tailBytesBuffered;
                        _tailBytesBuffered = 0;
                    }

                    BufferSegment newSegment = AllocateSegment(sizeHint);

                    _tail!.SetNext(newSegment);
                    _tail = newSegment;
                }
            }
        }

        private BufferSegment AllocateSegment(int sizeHint)
        {
            Debug.Assert(sizeHint >= 0);
            BufferSegment newSegment = CreateSegmentUnsynchronized();

            int maxSize = _maxPooledBufferSize;
            if (sizeHint <= maxSize)
            {
                // Use the specified pool as it fits. Specified pool is not null as maxSize == -1 if _pool is null.
                newSegment.SetOwnedMemory(_pool!.Rent(GetSegmentSize(sizeHint, maxSize)));
            }
            else
            {
                // Use the array pool
                int sizeToRequest = GetSegmentSize(sizeHint);
                newSegment.SetOwnedMemory(ArrayPool<byte>.Shared.Rent(sizeToRequest));
            }

            _tailMemory = newSegment.AvailableMemory;

            return newSegment;
        }

        private int GetSegmentSize(int sizeHint, int maxBufferSize = int.MaxValue)
        {
            // First we need to handle case where hint is smaller than minimum segment size
            sizeHint = Math.Max(_minimumBufferSize, sizeHint);
            // After that adjust it to fit into pools max buffer size
            var adjustedToMaximumSize = Math.Min(maxBufferSize, sizeHint);
            return adjustedToMaximumSize;
        }

        private BufferSegment CreateSegmentUnsynchronized()
        {
            if (_bufferSegmentPool.TryPop(out BufferSegment? segment))
            {
                return segment;
            }

            return new BufferSegment();
        }

        private void ReturnSegmentUnsynchronized(BufferSegment segment)
        {
            segment.Reset();
            if (_bufferSegmentPool.Count < MaxSegmentPoolSize)
            {
                _bufferSegmentPool.Push(segment);
            }
        }

        /// <inheritdoc />
        public override void CancelPendingFlush()
        {
            Cancel();
        }

        /// <inheritdoc />
        public override bool CanGetUnflushedBytes => true;

        /// <inheritdoc />
        public override void Complete(Exception? exception = null)
        {
            if (_isCompleted)
            {
                return;
            }

            _isCompleted = true;

            try
            {
                FlushInternal(writeToStream: exception == null, Timeout.InfiniteTimeSpan);
            }
            finally
            {
                _internalTokenSource?.Dispose();

                if (!_leaveOpen)
                {
                    InnerStream.Dispose();
                }
            }
        }

        public override async ValueTask CompleteAsync(Exception? exception = null)
        {
            if (_isCompleted)
            {
                return;
            }

            _isCompleted = true;

            try
            {
                await FlushAsyncInternal(writeToStream: exception == null, data: Memory<byte>.Empty).ConfigureAwait(false);
            }
            finally
            {
                _internalTokenSource?.Dispose();

                if (!_leaveOpen)
                {
#if (!NETSTANDARD2_0 && !NETFRAMEWORK)
                    await InnerStream.DisposeAsync().ConfigureAwait(false);
#else
                    InnerStream.Dispose();
#endif
                }
            }
        }

        /// <inheritdoc />
        public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
        {
            if (_bytesBuffered == 0)
            {
                return new ValueTask<FlushResult>(new FlushResult(isCanceled: false, isCompleted: false));
            }

            return FlushAsyncInternal(writeToStream: true, data: Memory<byte>.Empty, cancellationToken);
        }

        public PipeWriter PipeWriter => this;

        /// <inheritdoc />
        public FlushResult Flush(TimeSpan timeout)
        {
            if (_bytesBuffered == 0)
            {
                return new FlushResult(isCanceled: false, isCompleted: false);
            }

            return FlushInternal(writeToStream: true, timeout: timeout);
        }


        /// <inheritdoc />
        public override long UnflushedBytes => _bytesBuffered;

        public override ValueTask<FlushResult> WriteAsync(ReadOnlyMemory<byte> source, CancellationToken cancellationToken = default)
        {
            return FlushAsyncInternal(writeToStream: true, data: source, cancellationToken);
        }

        private void Cancel()
        {
            InternalTokenSource.Cancel();
        }

        int _flushing;

#if NETCOREAPP
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        private async ValueTask<FlushResult> FlushAsyncInternal(bool writeToStream, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            // Write all completed segments and whatever remains in the current segment
            // and flush the result.
            CancellationTokenRegistration reg = default;
            if (cancellationToken.CanBeCanceled)
            {
                reg = cancellationToken.UnsafeRegister(static state => ((StreamPipeWriter)state!).Cancel(), this);
            }

            using (reg)
            {
                if (_tailBytesBuffered > 0)
                {
                    Debug.Assert(_tail != null);

                    // Update any buffered data
                    _tail!.End += _tailBytesBuffered;
                    _tailBytesBuffered = 0;
                }

                CancellationToken localToken = InternalTokenSource.Token;
                try
                {
                    // To map conceptually to pipelines, only one flush can be active.
                    if (writeToStream && Interlocked.CompareExchange(ref _flushing, 1, 0) != 0)
                        ThrowHelper.ThrowInvalidOperationException_AlreadyFlushing();

                    BufferSegment? segment = _head;
                    while (segment != null)
                    {
                        BufferSegment returnSegment = segment;
                        segment = segment.NextSegment;

                        if (returnSegment.Length > 0 && writeToStream)
                        {
                            await InnerStream.WriteAsync(returnSegment.Memory, localToken).ConfigureAwait(false);
                        }

                        ReturnSegmentUnsynchronized(returnSegment);

                        // Update the head segment after we return the current segment
                        _head = segment;
                    }

                    if (writeToStream)
                    {
                        // Write data after the buffered data
                        if (data.Length > 0)
                        {
                            await InnerStream.WriteAsync(data, localToken).ConfigureAwait(false);
                        }

                        if (_bytesBuffered > 0 || data.Length > 0)
                        {
                            await InnerStream.FlushAsync(localToken).ConfigureAwait(false);
                        }
                    }

                    // Mark bytes as written *after* flushing
                    _head = null;
                    _tail = null;
                    _tailMemory = default;
                    _bytesBuffered = 0;

                    return new FlushResult(isCanceled: false, isCompleted: false);
                }
                catch (Exception ex) when (ex is ObjectDisposedException || (ex is IOException ioEx && ioEx.InnerException is ObjectDisposedException))
                {
                    return new FlushResult(isCompleted: true, isCanceled: false);
                }
                catch (OperationCanceledException)
                {
                    // Remove the cancellation token such that the next time Flush is called
                    // A new CTS is created.
                    lock (_lockObject)
                    {
                        _internalTokenSource = null;
                    }

                    if (localToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                    {
                        // Catch cancellation and translate it into setting isCanceled = true
                        return new FlushResult(isCanceled: true, isCompleted: false);
                    }

                    throw;
                }
                finally
                {
                    if (writeToStream)
                        Volatile.Write(ref _flushing, 0);
                }
            }
        }

        private FlushResult FlushInternal(bool writeToStream, TimeSpan timeout)
        {
            // Write all completed segments and whatever remains in the current segment
            // and flush the result.
            if (_tailBytesBuffered > 0)
            {
                Debug.Assert(_tail != null);

                // Update any buffered data
                _tail!.End += _tailBytesBuffered;
                _tailBytesBuffered = 0;
            }

            BufferSegment? segment = _head;
            long start = -1;
            var timeoutMillis = Timeout.Infinite;
            var previousTimeout = timeoutMillis;
            try
            {
                // To map conceptually to pipelines, only one flush can be active (this also allows us to mutate the WriteTimeout uncontested).
                if (writeToStream && Interlocked.CompareExchange(ref _flushing, 1, 0) != 0)
                    ThrowHelper.ThrowInvalidOperationException_AlreadyFlushing();

                if (writeToStream && _canTimeout)
                {
                    if (timeout != Timeout.InfiniteTimeSpan)
                        timeoutMillis = (int)timeout.TotalMilliseconds;
                    previousTimeout = _writeTimeout ?? InnerStream.WriteTimeout;
                    if (timeoutMillis != previousTimeout && timeoutMillis != 0 && timeoutMillis != Timeout.Infinite)
                    {
                        start = TickCount64Shim.Get();
                        InnerStream.WriteTimeout = timeoutMillis;
                    }
                }

                while (segment != null)
                {
                    BufferSegment returnSegment = segment;
                    segment = segment.NextSegment;

                    if (returnSegment.Length > 0 && writeToStream)
                    {
                        if (start != -1)
                        {
                            var elapsed = TickCount64Shim.Get() - start;
                            if (elapsed >= timeoutMillis)
                                throw new TimeoutException("The operation timed out.");
                            var remainingTimeout = timeoutMillis - (int)elapsed;
                            // Given this is rather expensive we only begin to do it once we start deviating fairly seriously (under 75%).
                            if (remainingTimeout / timeoutMillis * 100 <= 75)
                                InnerStream.WriteTimeout = remainingTimeout;
                        }

                        try
                        {
#if (!NETSTANDARD2_0 && !NETFRAMEWORK)
                            InnerStream.Write(returnSegment.Memory.Span);
#else
                            InnerStream.Write(returnSegment.Memory);
#endif
                        }
                        catch (Exception ex) when (ex is ObjectDisposedException || (ex is IOException ioEx && ioEx.InnerException is ObjectDisposedException))
                        {
                            return new FlushResult(isCompleted: true, isCanceled: false);
                        }
                        catch (IOException ex)
                        {
                            // We'll assume that if we're past our deadline a timeout was the reason for this exception, it sucks indeed.
                            // Stream has no contract to communicate an IOException was specifically because of a read/write/close timeout.
                            // This either means baking in all the different patterns (IOException wrapping SocketException etc.), or doing this.
                            if (start != -1 && TickCount64Shim.Get() - start >= timeoutMillis)
                                throw new TimeoutException("The operation has timed out", ex);
                            throw;
                        }
                    }

                    ReturnSegmentUnsynchronized(returnSegment);

                    // Update the head segment after we return the current segment
                    _head = segment;
                }

                if (_bytesBuffered > 0 && writeToStream)
                {
                    InnerStream.Flush();
                }
            }
            finally
            {
                if (start != -1)
                    InnerStream.WriteTimeout = previousTimeout;
                if (writeToStream)
                    Volatile.Write(ref _flushing, 0);
            }

            // Mark bytes as written *after* flushing
            _head = null;
            _tail = null;
            _tailMemory = default;
            _bytesBuffered = 0;
            return new(isCanceled: false, isCompleted: false);
        }
    }
}

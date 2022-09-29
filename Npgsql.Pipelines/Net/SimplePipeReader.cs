using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Pipelines;

/// <summary>
/// PipeReader wrapper that minimizes the cost of advance increments and simplifies forward movement and rereading the same buffer.
/// </summary>
sealed class SimplePipeReader
{
    long _readPosition;
    ReadOnlySequence<byte> _buffer = ReadOnlySequence<byte>.Empty;
    long _bufferLength;
    readonly IPipeReaderSyncSupport _readerSync;
    readonly PipeReader _reader;

    public SimplePipeReader(IPipeReaderSyncSupport reader)
    {
        _readerSync = reader;
        _reader = reader.PipeReader;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(long count)
    {
        var readPos = _readPosition + count;
        if (readPos > _bufferLength)
            ThrowAdvanceOutOfBounds();

        _readPosition = readPos;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryRead(int minimumSize, out ReadOnlySequence<byte> buffer)
    {
        if (_bufferLength != 0)
        {
            if (_readPosition == 0)
            {
                buffer = _buffer;
                return true;
            }

            _reader.AdvanceTo(_buffer.GetPosition(_readPosition));
        }

        if (_reader.TryRead(out var result))
        {
            var buf = result.Buffer;
            var bufferLength = buf.Length;
            if (bufferLength >= minimumSize)
            {
                buffer =_buffer = buf;
                _bufferLength = bufferLength;
                _readPosition = 0;
                return true;
            }

            // We didn't get enough data, fully consume so ReadAsync will wait.
            _reader.AdvanceTo(buf.Start, buf.End);
        }

        buffer = ReadOnlySequence<byte>.Empty;
        _readPosition = 0;
        _bufferLength = 0;
        return false;
    }

    /// <summary>Asynchronously reads a sequence of bytes from the current <see cref="System.IO.Pipelines.PipeReader" />.</summary>
    /// <param name="minimumSize">The minimum length that needs to be buffered in order to for the call to return.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see langword="default" />.</param>
    /// <returns>A <see cref="System.Threading.Tasks.ValueTask{T}" /> representing the asynchronous read operation.</returns>
    /// <remarks>
    ///     <para>
    ///     The call returns if the <see cref="System.IO.Pipelines.PipeReader" /> has read the minimumLength specified, or is cancelled or completed.
    ///     </para>
    ///     <para>
    ///     Passing a value of 0 for <paramref name="minimumSize" /> will return a <see cref="System.Threading.Tasks.ValueTask{T}" /> that will not complete until
    ///     further data is available. You should instead call <see cref="System.IO.Pipelines.PipeReader.TryRead" /> to avoid a blocking call.
    ///     </para>
    /// </remarks>
    public async ValueTask<ReadOnlySequence<byte>> ReadAtLeastAsync(int minimumSize, CancellationToken cancellationToken = default)
    {
        var result = await _reader.ReadAtLeastAsync(minimumSize, cancellationToken).ConfigureAwait(false);
        HandleReadResult(result);
        return result.Buffer;
    }

    public ReadOnlySequence<byte> ReadAtLeast(int minimumSize, TimeSpan timeout)
    {
        var result = _readerSync.ReadAtLeast(minimumSize, timeout);
        HandleReadResult(result);
        return result.Buffer;
    }

    void HandleReadResult(ReadResult result)
    {
        if (result.IsCompleted)
        {
            throw new ObjectDisposedException("Pipe was completed while waiting for more data.");
        }

        if (result.IsCanceled)
            throw new OperationCanceledException();

        // Clear the Reading state so TryRead can get a buffer again.
        _readPosition = 0;
        _buffer = result.Buffer;
        _bufferLength = result.Buffer.Length;
    }

    [DoesNotReturn]
    void ThrowAdvanceOutOfBounds() => throw new ArgumentOutOfRangeException("count", "Cannot read past buffer length");

    public ValueTask CompleteAsync(Exception? exception = null)
    {
        return _reader.CompleteAsync(exception);
    }

    public void Complete(Exception? exception = null)
    {
        _reader.Complete(exception);
    }
}

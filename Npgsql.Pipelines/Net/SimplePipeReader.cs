using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Pipelines;

/// <summary>
/// PipeReader wrapper that minimizes the cost of advance increments and simplifies forward movement.
/// </summary>
sealed class SimplePipeReader
{
    long _readPosition;
    ReadOnlySequence<byte> _buffer = ReadOnlySequence<byte>.Empty;
    long _bufferLength;
    readonly PipeReader _reader;

    public SimplePipeReader(PipeReader reader) => _reader = reader;

    public bool TryRead(long minimumSize, out ReadOnlySequence<byte> buffer)
    {
        if (_readPosition == 0 && _bufferLength >= minimumSize)
        {
            buffer = _buffer;
            return true;
        }

        if (_bufferLength != 0)
        {
            _reader.AdvanceTo(_buffer.GetPosition(_readPosition), _buffer.GetPosition(Math.Max(_readPosition, _bufferLength - 1)));
            _readPosition = 0;
            _buffer = ReadOnlySequence<byte>.Empty;
            _bufferLength = 0;
        }

        if (_reader.TryRead(out var result))
        {
            var buf = result.Buffer;
            var bufferLength = buf.Length;

            if (bufferLength >= minimumSize)
            {
                buffer =_buffer = buf;
                _bufferLength = bufferLength;
                return true;
            }

            _reader.AdvanceTo(buf.Start, buf.End);
        }

        buffer = ReadOnlySequence<byte>.Empty;
        return false;
    }

    public ValueTask WaitForDataAsync(long minimumSize, CancellationToken cancellationToken = default)
    {
        if (TryRead(minimumSize, out _))
            return new ValueTask();

        return WaitForDataAsyncCore(this, minimumSize, cancellationToken);

        static async ValueTask WaitForDataAsyncCore(SimplePipeReader instance, long minimumSize, CancellationToken cancellationToken)
        {
            while (true)
            {
                var result = await instance._reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                if (result.IsCompleted || result.IsCanceled)
                    throw result.IsCompleted ? new ObjectDisposedException("Pipe was completed while waiting for more data.") : new OperationCanceledException();

                var buffer = result.Buffer;
                var bufferLength = buffer.Length;
                if (bufferLength >= minimumSize)
                {
                    // Immediately advance so TryRead can reread.
                    instance._reader.AdvanceTo(buffer.Start, buffer.GetPosition(bufferLength - 1));
                    break;
                }

                // Keep buffering until we get more data
                instance._reader.AdvanceTo(buffer.Start, buffer.End);
            }
        }
    }

    public void Advance(long bytes) => _readPosition += bytes;

    public void Complete(Exception? exception = null)
    {
        _reader.Complete(exception);
    }
}

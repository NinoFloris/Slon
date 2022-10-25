using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace System.IO;

#if NETSTANDARD2_0

static class StreamExtensions
{
    public static void Write(this Stream stream, ReadOnlyMemory<byte> buffer)
    {
        if (MemoryMarshal.TryGetArray(buffer, out ArraySegment<byte> array))
        {
            stream.Write(array.Array!, array.Offset, array.Count);
        }
        else
        {
            byte[] sharedBuffer = ArrayPool<byte>.Shared.Rent(buffer.Length);
            try
            {
                buffer.Span.CopyTo(sharedBuffer);
                stream.Write(sharedBuffer, 0, buffer.Length);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(sharedBuffer);
            }
        }
    }

    public static ValueTask WriteAsync(this Stream stream, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (MemoryMarshal.TryGetArray(buffer, out ArraySegment<byte> array))
        {
            return new ValueTask(stream.WriteAsync(array.Array, array.Offset, array.Count, cancellationToken));
        }
        else
        {
            byte[] sharedBuffer = ArrayPool<byte>.Shared.Rent(buffer.Length);
            buffer.Span.CopyTo(sharedBuffer);
            return new ValueTask(FinishWriteAsync(stream.WriteAsync(sharedBuffer, 0, buffer.Length, cancellationToken), sharedBuffer));
        }
    }

    private static async Task FinishWriteAsync(Task writeTask, byte[] localBuffer)
    {
        try
        {
            await writeTask.ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(localBuffer);
        }
    }

    public static async ValueTask<int> ReadAsync(this Stream stream, Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (MemoryMarshal.TryGetArray(buffer, out ArraySegment<byte> array))
        {
            return await stream.ReadAsync(array.Array!, array.Offset, array.Count, cancellationToken);
        }
        else
        {
            var localArray = ArrayPool<byte>.Shared.Rent(buffer.Length);
            var read = await stream.ReadAsync(localArray, 0, buffer.Length, cancellationToken);
            localArray.AsSpan(0, read).CopyTo(buffer.Span);
            buffer.Span.CopyTo(localArray);
            ArrayPool<byte>.Shared.Return(localArray);
            return read;
        }
    }
}

#endif

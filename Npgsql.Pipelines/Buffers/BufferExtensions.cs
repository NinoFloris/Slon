using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace System.Buffers;

internal static class BufferExtensions
{
    internal static void WriteByte<T>(ref this BufferWriter<T> buffer, byte b)
        where T : IBufferWriter<byte>
    {
        buffer.Ensure(1);
        buffer.Span[0] = b;
        buffer.Advance(1);
    }

    internal static Encoder? WriteEncoded<T>(ref this BufferWriter<T> buffer, ReadOnlySpan<char> data, Encoding encoding, Encoder? encoder = null)
        where T : IBufferWriter<byte>
    {
        if (data.IsEmpty)
            return null;

        var dest = buffer.Span;
        var sourceLength = encoding.GetByteCount(data);
        // Fast path, try encoding to the available memory directly
        if (encoder is null && sourceLength <= dest.Length)
        {
            encoding.GetBytes(data, dest);
            buffer.Advance(sourceLength);
            return null;
        }
        else
        {
            return WriteEncodedMultiWrite(ref buffer, data, sourceLength, encoding);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    static Encoder? WriteEncodedMultiWrite<T>(ref this BufferWriter<T> buffer, ReadOnlySpan<char> data, int encodedLength, Encoding encoding, Encoder? enc = null)
        where T : IBufferWriter<byte>
    {
        var source = data;
        var totalBytesUsed = 0;
        var encoder = enc ?? encoding.GetEncoder();
        var minBufferSize = encoding.GetMaxByteCount(1);
        buffer.Ensure(minBufferSize);
        var bytes = buffer.Span;
        var completed = false;

        // This may be a bug, but encoder.Convert returns completed = true for UTF7 too early.
        // Therefore, we check encodedLength - totalBytesUsed too.
        while (!completed || encodedLength - totalBytesUsed != 0)
        {
            // Zero length spans are possible, though unlikely.
            // encoding.Convert and .Advance will both handle them so we won't special case for them.
            encoder.Convert(source, bytes, flush: true, out var charsUsed, out var bytesUsed, out completed);
            buffer.Advance(bytesUsed);

            totalBytesUsed += bytesUsed;
            if (totalBytesUsed >= encodedLength)
            {
                Debug.Assert(totalBytesUsed == encodedLength);
                // Encoded everything
                break;
            }

            source = source.Slice(charsUsed);

            // Get new span, more to encode.
            buffer.Ensure(minBufferSize);
            bytes = buffer.Span;
        }

        return encoder;
    }
}

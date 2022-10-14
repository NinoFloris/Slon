using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace System.Buffers;

static class BufferWriter
{
    // About the default MTU payload size, not sure how much, if any, it helps.
    public const int DefaultCommitThreshold = 1450;
}

interface ICopyableBufferWriter<T> : IBufferWriter<T>
{
    void CopyTo(IBufferWriter<T> destination);
}

static class BufferExtensions
{
    public static void CopyTo<T, TWriter>(ref this BufferWriter<T> buffer, ref BufferWriter<TWriter> otherBuffer) where T : ICopyableBufferWriter<byte> where TWriter : IBufferWriter<byte>
    {
        buffer.Commit();
        otherBuffer.Commit();
        buffer.Output.CopyTo(otherBuffer.Output);
        otherBuffer.AdvanceCommitted(buffer.BytesCommitted);
    }

    public static void WriteRaw<T>(ref this BufferWriter<T> buffer, ReadOnlySpan<byte> value) where T : IBufferWriter<byte>
    {
        buffer.Write(value);
    }

    public static void WriteShort<T>(ref this BufferWriter<T> buffer, short value)  where T : IBufferWriter<byte>
    {
        buffer.Ensure(sizeof(short));
        BinaryPrimitives.WriteInt16BigEndian(buffer.Span, value);
        buffer.Advance(sizeof(short));
    }

    public static void WriteInt<T>(ref this BufferWriter<T> buffer, int value) where T : IBufferWriter<byte>
    {
        buffer.Ensure(sizeof(int));
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span, value);
        buffer.Advance(sizeof(int));
    }

    public static void WriteCString<T>(ref this BufferWriter<T> buffer, string value) where T : IBufferWriter<byte>
    {
        if (value is not "")
            buffer.WriteEncoded(value.AsSpan(), Encoding.UTF8);
        buffer.WriteByte(0);
    }

    public static void WriteByte<T>(ref this BufferWriter<T> buffer, byte b)
        where T : IBufferWriter<byte>
    {
        buffer.Ensure(sizeof(byte));
        buffer.Span[0] = b;
        buffer.Advance(1);
    }

    public static Encoder? WriteEncoded<T>(ref this BufferWriter<T> buffer, ReadOnlySpan<char> data, Encoding encoding, Encoder? encoder = null)
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

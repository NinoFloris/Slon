using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;

namespace Slon.Buffers;

static class SequenceReaderExtensions
{
    // Read a bool from a byte, only 0 and 1 will make this method return true.
    public static OperationStatus TryReadBool(this ref SequenceReader<byte> reader, out bool value)
    {
        var copy = reader;

        if (!reader.TryRead(out var b) || b > 1)
        {
            reader = copy;
            value = false;
            return b > 1 ? OperationStatus.InvalidData : OperationStatus.NeedMoreData;
        }

        value = b == 1;
        return OperationStatus.Done;
    }

    public static bool TryRead(this ref SequenceReader<byte> reader, out sbyte value)
    {
        UnsafeShim.SkipInit(out value);
        return reader.TryRead(out Unsafe.As<sbyte, byte>(ref value));
    }

    public static bool TryReadBigEndian(this ref SequenceReader<byte> reader, out ushort value)
    {
        UnsafeShim.SkipInit(out value);
        return reader.TryReadBigEndian(out Unsafe.As<ushort, short>(ref value));
    }

    public static bool TryReadBigEndian(this ref SequenceReader<byte> reader, out uint value)
    {
        UnsafeShim.SkipInit(out value);
        return reader.TryReadBigEndian(out Unsafe.As<uint, int>(ref value));
    }

    public static bool TryReadBigEndian(this ref SequenceReader<byte> reader, out ulong value)
    {
        UnsafeShim.SkipInit(out value);
        return reader.TryReadBigEndian(out Unsafe.As<ulong, long>(ref value));
    }

    public static OperationStatus TryReadCString(this ref SequenceReader<byte> reader, Encoding encoding, out string? value)
    {
        if (reader.TryReadTo(out ReadOnlySequence<byte> strBytes, 0))
        {
            try
            {
                value = encoding.GetString(strBytes);
                return OperationStatus.Done;
            }
            catch(DecoderFallbackException)
            {
                value = null;
                return OperationStatus.InvalidData;
            }
        }

        value = null;
        return OperationStatus.NeedMoreData;
    }

    public static OperationStatus TryReadCString(this ref SequenceReader<byte> reader, Encoding encoding, scoped Span<char> destination)
    {
        if (reader.TryReadTo(out ReadOnlySequence<byte> strBytes, 0))
        {
            try
            {
                encoding.GetChars(strBytes, destination);
                return OperationStatus.Done;
            }
            catch(DecoderFallbackException)
            {
                return OperationStatus.InvalidData;
            }
            catch(ArgumentException)
            {
                return OperationStatus.DestinationTooSmall;
            }
        }

        return OperationStatus.NeedMoreData;
    }

    public static bool TryReadCStringBuffer(this ref SequenceReader<byte> reader, out ReadOnlySequence<byte> bytes) => reader.TryReadTo(out bytes, 0);
}

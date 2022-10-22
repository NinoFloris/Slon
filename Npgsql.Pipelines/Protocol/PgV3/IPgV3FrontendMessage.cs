using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Npgsql.Pipelines.Protocol.PgV3;

enum FrontendCode: byte
{
    Describe = (byte) 'D',
    Sync = (byte) 'S',
    Execute = (byte) 'E',
    Parse = (byte) 'P',
    Bind = (byte) 'B',
    Close = (byte) 'C',
    Query = (byte) 'Q',
    CopyData = (byte) 'd',
    CopyDone = (byte) 'c',
    CopyFail = (byte) 'f',
    Terminate = (byte) 'X',
    Password = (byte) 'p',
}

struct PgV3FrontendHeader: IFrontendHeader<PgV3FrontendHeader>
{
    readonly FrontendCode _code;
    int _length;

    PgV3FrontendHeader(FrontendCode code, int length)
    {
        _code = code;
        _length = length;
    }

    public int HeaderLength => PgV3Header.ByteCount;

    public int Length
    {
        get => _length + HeaderLength;
        set
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), "Value cannot be negative.");

            _length = value;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        buffer.WriteByte((byte)_code);
        buffer.WriteInt(_length + sizeof(int));
    }

    public static PgV3FrontendHeader Create(FrontendCode code, int length)
    {
        if (length < 0)
            ThrowArgumentOutOfRange();

        return new PgV3FrontendHeader(code, length);

        void ThrowArgumentOutOfRange() => throw new ArgumentOutOfRangeException(nameof(length), "Length cannot be negative");
    }
}

interface IPgV3FrontendMessage: IFrontendMessage<PgV3FrontendHeader>
{
}

interface IPgV3StreamingFrontendMessage: IStreamingFrontendMessage<PgV3FrontendHeader>
{
}

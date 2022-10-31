using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using Npgsql.Pipelines.Protocol.PgV3.Types;

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
    const int ByteCount = PgV3Header.ByteCount;
    readonly FrontendCode _code;
    int _length;

    PgV3FrontendHeader(FrontendCode code, int length)
    {
        _code = code;
        _length = length;
    }

    public int Length
    {
        get => _length + ByteCount;
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

sealed class PgV3ProtocolParameterType: ProtocolParameterType
{
    protected override bool GetIsBinary()
    {
        throw new NotImplementedException();
    }

    protected override int? GetLength()
    {
        throw new NotImplementedException();
    }

    public Parameter Parameter { get; }
    public FormatCode FormatCode { get => IsBinary ? FormatCode.Binary : FormatCode.Text; }
}

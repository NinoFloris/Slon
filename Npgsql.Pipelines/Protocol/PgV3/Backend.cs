using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines.Protocol.PgV3;

enum BackendCode: byte
{
    AuthenticationRequest   = (byte)'R',
    BackendKeyData          = (byte)'K',
    BindComplete            = (byte)'2',
    CloseComplete           = (byte)'3',
    CommandComplete         = (byte)'C',
    CopyData                = (byte)'d',
    CopyDone                = (byte)'c',
    CopyBothResponse        = (byte)'W',
    CopyInResponse          = (byte)'G',
    CopyOutResponse         = (byte)'H',
    DataRow                 = (byte)'D',
    EmptyQueryResponse      = (byte)'I',
    ErrorResponse           = (byte)'E',
    FunctionCall            = (byte)'F',
    FunctionCallResponse    = (byte)'V',
    NoData                  = (byte)'n',
    NoticeResponse          = (byte)'N',
    NotificationResponse    = (byte)'A',
    ParameterDescription    = (byte)'t',
    ParameterStatus         = (byte)'S',
    ParseComplete           = (byte)'1',
    PasswordPacket          = (byte)' ',
    PortalSuspended         = (byte)'s',
    ReadyForQuery           = (byte)'Z',
    RowDescription          = (byte)'T',
}

readonly record struct PgV3Header: IHeader<PgV3Header>
{
    public const int ByteCount = sizeof(uint) + sizeof(BackendCode);

    readonly uint _length;
    readonly BackendCode _code;

    PgV3Header(BackendCode code, uint length)
    {
        _code = code;
        _length = length;
    }

    public BackendCode Code => _code;
    public int HeaderLength => ByteCount;
    public uint Length => _length;
    long IHeader<PgV3Header>.Length => _length;

    public bool IsDefault => _code == 0;
    public bool IsAsyncResponse => _code is BackendCode.NoticeResponse or BackendCode.NotificationResponse or BackendCode.ParameterStatus;
    public bool TypeEquals(in PgV3Header other) => other._code == _code;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryParse(ReadOnlySpan<byte> span, out PgV3Header header)
    {
        if (span.Length < ByteCount)
        {
            header = default;
            return false;
        }

        ref var first = ref MemoryMarshal.GetReference(span);
        var code = (BackendCode)first;

        if (BackendMessage.DebugEnabled)
            ThrowIfNotDefined(code);

        var length = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref first, 1));
        if (BitConverter.IsLittleEndian)
            length = BinaryPrimitives.ReverseEndianness(length);
        length += sizeof(byte);

        header = new PgV3Header(code, length);
        return true;

        static void ThrowIfNotDefined(BackendCode code)
        {
            if (!EnumShim.IsDefined(code))
                throw new InvalidDataException("Unknown backend code: " + code);
        }
    }

    public static bool TryParse(in ReadOnlySequence<byte> buffer, out PgV3Header header)
    {
        Span<byte> span = stackalloc byte[ByteCount];
        if (!TryParse(buffer.GetFirstSpan(), out header) && (!ReadOnlySequenceExtensions.TryCopySlow(buffer, span) || !TryParse(span, out header)))
        {
            header = default;
            return false;
        }

        return true;
    }

    public bool TryParse(in ReadOnlySpan<byte> unreadSpan, in ReadOnlySequence<byte> buffer, long bufferStart, out PgV3Header header)
    {
        Span<byte> span = stackalloc byte[ByteCount];
        if (!TryParse(unreadSpan, out header) && (!ReadOnlySequenceExtensions.TryCopySlow(buffer.Slice(buffer.GetPosition(bufferStart)), span) || !TryParse(span, out header)))
        {
            header = default;
            return false;
        }

        return true;
    }

    public static PgV3Header CreateType(BackendCode code) => new(code, 0);
    public static PgV3Header Create(BackendCode code, uint length) => new(code, length);
}

interface IPgV3BackendMessage: IBackendMessage<PgV3Header>
{
}

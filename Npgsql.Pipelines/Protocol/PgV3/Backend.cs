using System;
using System.Buffers;
using System.Buffers.Binary;
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

        if (BackendMessage.DebugEnabled && !EnumShim.IsDefined(code))
            ThrowNotDefined(code);

        var length = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref first, 1));
        if (BitConverter.IsLittleEndian)
            length = BinaryPrimitives.ReverseEndianness(length);
        length += sizeof(byte);

        header = new PgV3Header(code, length);
        return true;

        static void ThrowNotDefined(BackendCode code) => throw new ArgumentOutOfRangeException(nameof(code), code, "Unknown backend code");
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

// Refined reimplementations based on the actual header type.
static class MessageReaderExtensions
{
    /// <summary>
    /// Skip messages until header.Code does not equal code.
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="code"></param>
    /// <param name="matchingHeader"></param>
    /// <param name="status">returned if we found an async response or couldn't skip past 'code' yet.</param>
    /// <returns>Returns true if skip succeeded, false if it could not skip past 'code' yet.</returns>
    public static bool SkipSimilar(this ref MessageReader<PgV3Header> reader, BackendCode code, out ReadStatus status)
        => reader.SkipSimilar(PgV3Header.CreateType(code), out status);

    public static bool IsExpected(this ref MessageReader<PgV3Header> reader, BackendCode code, out ReadStatus status, bool ensureBuffered = false)
        => reader.IsExpected(PgV3Header.CreateType(code), out status, ensureBuffered);

    public static bool MoveNextAndIsExpected(this ref MessageReader<PgV3Header> reader, BackendCode code, out ReadStatus status, bool ensureBuffered = false)
        => reader.MoveNextAndIsExpected(PgV3Header.CreateType(code), out status, ensureBuffered);

    public static bool ReadMessage<T>(this ref MessageReader<PgV3Header> reader, out ReadStatus status)
        where T : struct, IBackendMessage<PgV3Header>
    {
        status = new T().Read(ref reader);
        return status == ReadStatus.Done;
    }
}

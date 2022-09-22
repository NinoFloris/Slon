using System;
using System.Text;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Npgsql.Pipelines;

[StructLayout(LayoutKind.Auto)]
readonly struct MessageHeader
{
    // byte - code
    // int4 - length
    public const int CodeAndLengthByteCount = 5;

    readonly BackendCode _code;
    // We use uint because technically the max length for a postgres message is one more byte than we would be able to store
    // As we always add the code byte to the length. To compensate we use uint.
    readonly uint _length;

    public MessageHeader(BackendCode code, uint length)
    {
        _code = code;
        _length = length;
    }

    [DoesNotReturn]
    void ThrowInvalidOperationNotInitialized() => throw new InvalidOperationException("This operation cannot be performed on a default instance of MessageHeader.");

    public BackendCode Code
    {
        get
        {
            if (IsDefault)
                ThrowInvalidOperationNotInitialized();
            return _code;
        }
    }

    public uint Length
    {
        get
        {
            if (IsDefault)
                ThrowInvalidOperationNotInitialized();
            return _length;
        }
    }

    public uint PayloadLength => Length - CodeAndLengthByteCount;

    // Length can never be less than CodeAndLengthByteCount.
    public bool IsDefault => _length == 0;

    public static bool TryParse(ref SequenceReader<byte> reader, out BackendCode code, out uint messageLength)
    {
        Span<byte> header = stackalloc byte[MessageHeader.CodeAndLengthByteCount];
        if (!reader.TryCopyTo(header))
        {
            code = default;
            messageLength = default;
            return false;
        }

        code = (BackendCode)header[0];
        messageLength = (uint)BinaryPrimitives.ReadInt32BigEndian(header.Slice(1)) + 1;

        if (BackendMessageDebug.Enabled && !EnumShim.IsDefined(code))
            throw new Exception("Unknown backend code: " + code);

        return true;
    }
}

readonly struct MessageStartOffset
{
    readonly long _offset;

    public MessageStartOffset(long offset) => _offset = offset;

    public long GetOffset() => _offset;
}

[DebuggerDisplay("Code = {Current.Code}, Length = {Current.Length}")]
ref struct MessageReader
{
    readonly ReadOnlySequence<byte> _sequence;
    public SequenceReader<byte> Reader;
    MessageStartOffset _messageStart;

    MessageReader(ReadOnlySequence<byte> sequence, MessageStartOffset messageStart)
    {
        _sequence = sequence;
        _messageStart = messageStart;
        Reader = new SequenceReader<byte>(_sequence);
    }

    public MessageHeader Current { [MethodImpl(MethodImplOptions.AggressiveInlining)] get; private set; }

    public ReadOnlySpan<byte> UnreadSpan => Reader.UnreadSpan;
    public ReadOnlySequence<byte> Sequence => _sequence;

    public long Consumed => Reader.Consumed;
    public uint CurrentConsumed => (uint)(Consumed - CurrentStart.GetOffset());

    public bool IsCurrentBuffered => Reader.Length >= CurrentStart.GetOffset() + Current.Length;

    // Having to subtract start is a bug in ReadOnlySequence https://github.com/dotnet/runtime/issues/75866
    static readonly bool HasGetOffsetBug = CheckGetOffsetBug();

    static bool CheckGetOffsetBug()
    {
        var seq = new ReadOnlySequence<byte>(new byte[1], 1, 0);
        return seq.GetOffset(seq.Start) != 0;
    }

    long GetOffsetShim(SequencePosition position) => Sequence.GetOffset(position) - (HasGetOffsetBug ? Sequence.Start.GetInteger() : 0);

    public MessageStartOffset CurrentStart => _messageStart;

    public static MessageReader Create(in ReadOnlySequence<byte> sequence) => new(sequence, new MessageStartOffset(0));

    /// <summary>
    /// Recreate a reader over the same (or the same starting point) sequence with resumption data.
    /// </summary>
    /// <param name="sequence"></param>
    /// <param name="resumptionData"></param>
    /// <param name="consumed"></param>
    /// <returns></returns>
    public static MessageReader Create(in ReadOnlySequence<byte> sequence, in ResumptionData resumptionData, long consumed)
    {
        var reader = new MessageReader(sequence, new MessageStartOffset(resumptionData.MessageIndex));
        reader.Current = resumptionData.Header;
        reader.Reader.Advance(consumed);
        return reader;
    }

    /// <summary>
    /// Create a reader over a new sequence, one that starts after the consumed point of the previous one.
    /// </summary>
    /// <param name="sequence"></param>
    /// <param name="resumptionData"></param>
    /// <returns></returns>
    public static MessageReader Resume(in ReadOnlySequence<byte> sequence, in ResumptionData resumptionData)
    {
        var reader = new MessageReader(sequence, new MessageStartOffset(-resumptionData.MessageIndex));
        reader.Current = resumptionData.Header;
        return reader;
    }

    public ResumptionData GetResumptionData()
    {
        if (Current.IsDefault)
            throw new InvalidOperationException("Cannot get resumption data, enumeration hasn't started.");
        return new(Current, CurrentConsumed);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MoveNext()
    {
        if (!Current.IsDefault && !ConsumeCurrent())
            return false;

        if (!MessageHeader.TryParse(ref Reader, out var code, out var length))
            return false;

        _messageStart = new MessageStartOffset(GetOffsetShim(Reader.Position));
        Current = new MessageHeader(code, length);
        Reader.Advance(MessageHeader.CodeAndLengthByteCount);
        return true;
    }

    /// <summary>
    /// Consume always succeeds if Current is buffered and never if it isn't.
    /// </summary>
    /// <returns>A bool signalling whether the reader advanced past the current message, this may leave an empty reader.</returns>
    public bool ConsumeCurrent()
    {
        if (!IsCurrentBuffered)
            return false;
        if (_messageStart.GetOffset() + MessageHeader.CodeAndLengthByteCount == Consumed)
            Reader.Advance(Current.PayloadLength);
        else
        {
            // Friendly error, otherwise ArgumentOutOfRange is thrown from Advance.
            if (BackendMessageDebug.Enabled && _messageStart.GetOffset() + Current.Length < Reader.Consumed)
                throw new InvalidOperationException("Can't consume current message as the reader already advanced past this message boundary.");

            Reader.Advance(_messageStart.GetOffset() + Current.Length - Reader.Consumed);
        }

        return true;
    }

    public void MoveTo(MessageStartOffset offset)
    {
        // Workaround https://github.com/dotnet/runtime/issues/68774
        if (Consumed - offset.GetOffset() > 0)
            Reader.Rewind(Consumed - offset.GetOffset());

        if (!MessageHeader.TryParse(ref Reader, out var code, out var messageLength))
            throw new ArgumentOutOfRangeException(nameof(offset), "Given offset is not valid for this reader.");
        Current = new MessageHeader(code, messageLength);
        _messageStart = new MessageStartOffset(GetOffsetShim(Reader.Position));
    }

    public readonly struct ResumptionData
    {
        public ResumptionData(MessageHeader header, uint messageIndex)
        {
            Header = header;
            MessageIndex = messageIndex;
        }

        // The current message at the time of the suspend.
        public MessageHeader Header { get; }
        // Where we need to start relative to the message in Header.
        public uint MessageIndex { get; }
    }
}

static class MessageReaderExtensions
{
    /// <summary>
    /// Skip messages until code does not equal Current.Code.
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="code"></param>
    /// <returns>Returns true if skip succeeded, false if it could not skip past 'code' yet.</returns>
    public static bool SkipSimilar(this ref MessageReader reader, BackendCode code)
    {
        bool moved = true;
        while (reader.Current.Code == code && (moved = reader.MoveNext()))
        {}

        return moved;
    }

    public static bool TryReadCString(this ref MessageReader reader, [NotNullWhen(true)]out string? value)
    {
        if (reader.Reader.TryReadTo(out ReadOnlySequence<byte> strBytes, 0))
        {
            value = PgEncoding.RelaxedUTF8.GetString(strBytes);
            return true;
        }

        value = null;
        return false;
    }

    public static bool TryReadShort(this ref MessageReader reader, out short value) => reader.Reader.TryReadBigEndian(out value);
    public static bool TryReadInt(this ref MessageReader reader, out int value) => reader.Reader.TryReadBigEndian(out value);

    public static bool TryReadUInt(this ref MessageReader reader, out uint value)
    {
        UnsafeShim.SkipInit(out value);
        return reader.Reader.TryReadBigEndian(out Unsafe.As<uint,int>(ref value));
    }

    public static bool TryReadByte(this ref MessageReader reader, out byte value) => reader.Reader.TryRead(out value);

    public static bool TryReadBool(this ref MessageReader reader, out bool value)
    {
        if (!TryReadByte(ref reader, out var b))
        {
            value = false;
            return false;
        }

        const byte mask = 00000001;
        b = (byte)(b & mask);
        value = Unsafe.As<byte, bool>(ref b);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsExpected(this ref MessageReader reader, BackendCode code, out ReadStatus status, bool ensureBuffered = false)
    {
        if (reader.Current.Code != code)
        {
            status = ResolveStatus(code);
            return false;
        }

        if (ensureBuffered && reader.Reader.Remaining < reader.Current.PayloadLength)
        {
            status = ReadStatus.NeedMoreData;
            return false;
        }

        status = default;
        return true;

        [MethodImpl(MethodImplOptions.NoInlining)]
        static ReadStatus ResolveStatus(BackendCode code) =>
            code is BackendCode.NoticeResponse or BackendCode.NotificationResponse or BackendCode.ParameterStatus
                ? ReadStatus.AsyncResponse : ReadStatus.InvalidData;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool MoveNextAndIsExpected(this ref MessageReader reader, BackendCode code, out ReadStatus status, bool ensureBuffered = false)
    {
        if (!reader.MoveNext())
        {
            status = ReadStatus.NeedMoreData;
            return false;
        }

        return reader.IsExpected(code, out status, ensureBuffered);
    }
}

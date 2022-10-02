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
    public const int ByteCount = 5;

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

    public uint PayloadLength => Length - ByteCount;

    // Length can never be less than CodeAndLengthByteCount.
    public bool IsDefault => _length == 0;

    public static bool TryParse(ref ReadOnlySequence<byte> buffer, out BackendCode code, out uint messageLength)
    {
        Span<byte> header = stackalloc byte[ByteCount];

        var firstSpan = buffer.First.Span;
        if (firstSpan.Length >= header.Length)
            firstSpan.Slice(0, header.Length).CopyTo(header);
        else
        {
            if (!TryCopySlow(ref buffer, header))
            {
                code = default;
                messageLength = default;
                return false;
            }
        }

        code = (BackendCode)header[0];
        messageLength = BinaryPrimitives.ReadUInt32BigEndian(header.Slice(1)) + 1;

        if (BackendMessageDebug.Enabled && !EnumShim.IsDefined(code))
            throw new Exception("Unknown backend code: " + code);

        return true;

        static bool TryCopySlow(ref ReadOnlySequence<byte> buffer, Span<byte> destination)
        {
            if (buffer.Length < destination.Length)
                return false;

            var firstSpan = buffer.First.Span;
            Debug.Assert(firstSpan.Length < destination.Length);
            firstSpan.CopyTo(destination);
            int copied = firstSpan.Length;

            SequencePosition next = buffer.GetPosition(firstSpan.Length);
            while (buffer.TryGet(ref next, out ReadOnlyMemory<byte> nextSegment, true))
            {
                if (nextSegment.Length > 0)
                {
                    var nextSpan = nextSegment.Span;
                    int toCopy = Math.Min(nextSpan.Length, destination.Length - copied);
                    nextSpan.Slice(0, toCopy).CopyTo(destination.Slice(copied));
                    copied += toCopy;
                    if (copied >= destination.Length)
                    {
                        break;
                    }
                }
            }

            return true;
        }
    }

    public static bool TryParse(ref SequenceReader<byte> reader, out BackendCode code, out uint messageLength)
    {
        Span<byte> header = stackalloc byte[ByteCount];
        if (!reader.TryCopyTo(header))
        {
            code = default;
            messageLength = default;
            return false;
        }
        code = (BackendCode)header[0];
        messageLength = BinaryPrimitives.ReadUInt32BigEndian(header.Slice(1)) + 1;

        if (BackendMessageDebug.Enabled && !EnumShim.IsDefined(code))
            throw new Exception("Unknown backend code: " + code);

        reader.Advance(ByteCount);
        return true;
    }
}

readonly struct MessageStartOffset
{
    [Flags]
    enum MessageStartOffsetFlags
    {
        None = 0,
        ResumptionOffset = 1,
        BeforeFirstMove = 2
    }

    readonly MessageStartOffsetFlags _flags;
    readonly uint _offset;

    MessageStartOffset(uint offset, MessageStartOffsetFlags flags)
        : this(offset)
    {
        _flags = flags;
    }
    public MessageStartOffset(uint offset) => _offset = offset;

    public bool IsResumption => (_flags & MessageStartOffsetFlags.ResumptionOffset) != 0;
    public bool IsBeforeFirstMove => (_flags & MessageStartOffsetFlags.BeforeFirstMove) != 0;

    public long GetOffset() => IsResumption ? -_offset : _offset;

    public static MessageStartOffset Recreate(uint offset) => new(offset, MessageStartOffsetFlags.BeforeFirstMove);
    public static MessageStartOffset Resume(uint offset) => new(offset, MessageStartOffsetFlags.ResumptionOffset | MessageStartOffsetFlags.BeforeFirstMove);
    public MessageStartOffset MovedNext() => new(_offset, _flags & ~MessageStartOffsetFlags.BeforeFirstMove);
}

[DebuggerDisplay("Code = {Current.Code}, Length = {Current.Length}")]
ref struct MessageReader
{
    public MessageStartOffset CurrentStart;
    public readonly ReadOnlySequence<byte> Sequence;
    public SequenceReader<byte> Reader;
    public MessageHeader Current;

    MessageReader(ReadOnlySequence<byte> sequence, MessageStartOffset currentStart)
    {
        Sequence = sequence;
        CurrentStart = currentStart;
        Reader = new SequenceReader<byte>(Sequence);
    }

    public readonly long Consumed => Reader.Consumed;
    public readonly uint CurrentConsumed => (uint)(Consumed - CurrentStart.GetOffset());
    public readonly bool IsCurrentBuffered => Reader.Length >= CurrentStart.GetOffset() + Current.Length;

    public static MessageReader Create(scoped in ReadOnlySequence<byte> sequence) => new(sequence, new MessageStartOffset(0));

    /// <summary>
    /// Recreate a reader over the same (or the same starting point) sequence with resumption data.
    /// </summary>
    /// <param name="sequence"></param>
    /// <param name="resumptionData"></param>
    /// <param name="consumed"></param>
    /// <returns></returns>
    public static MessageReader Create(scoped in ReadOnlySequence<byte> sequence, scoped in ResumptionData resumptionData, long consumed)
    {
        if (consumed - resumptionData.MessageIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(resumptionData), "Resumption data carries an invalid MessageIndex for the given consumed value, as its larger than the value of consumed.");

        var reader = new MessageReader(sequence, MessageStartOffset.Recreate((uint)consumed - resumptionData.MessageIndex));
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
    public static MessageReader Resume(scoped in ReadOnlySequence<byte> sequence, scoped in ResumptionData resumptionData)
    {
        var reader = new MessageReader(sequence, MessageStartOffset.Resume(resumptionData.MessageIndex));
        reader.Current = resumptionData.Header;
        return reader;
    }

    public readonly ResumptionData GetResumptionData() => new(Current, CurrentConsumed);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MoveNext()
    {
        if (CurrentStart.IsBeforeFirstMove)
        {
            CurrentStart = CurrentStart.MovedNext();
            return true;
        }

        if (!Current.IsDefault && !ConsumeCurrent())
            return false;

        if (!MessageHeader.TryParse(ref Reader, out var code, out var length))
        {
            // This is here to make sure resumption data accurately reflects that we are on a new message, we just can't read it yet.
            CurrentStart = default;
            Current = default;
            return false;
        }

        CurrentStart = new MessageStartOffset((uint)Reader.Consumed - MessageHeader.ByteCount);
        Current = new MessageHeader(code, length);
        return true;
    }

    /// <summary>
    /// Consume always succeeds if Current is buffered and never if it isn't.
    /// </summary>
    /// <returns>A bool signalling whether the reader advanced past the current message, this may leave an empty reader.</returns>
    public bool ConsumeCurrent()
    {
        if (!IsCurrentBuffered || CurrentStart.IsBeforeFirstMove)
            return false;

        // Friendly error, otherwise ArgumentOutOfRange is thrown from Advance.
        if (BackendMessageDebug.Enabled && Reader.Consumed > CurrentStart.GetOffset() + Current.Length)
            throw new InvalidOperationException("Can't consume current message as the reader already advanced past this message boundary.");

        Reader.Advance(CurrentStart.GetOffset() + Current.Length - Reader.Consumed);

        return true;
    }

    public void MoveTo(in MessageStartOffset offset)
    {
        // Workaround https://github.com/dotnet/runtime/issues/68774
        if (Consumed - offset.GetOffset() > 0)
            Reader.Rewind(Consumed - offset.GetOffset());

        if (!MessageHeader.TryParse(ref Reader, out var code, out var messageLength))
            throw new ArgumentOutOfRangeException(nameof(offset), "Given offset is not valid for this reader.");
        Current = new MessageHeader(code, messageLength);
        CurrentStart = new MessageStartOffset((uint)Reader.Consumed);
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

        public bool IsDefault => Header.IsDefault;
    }
}

static class MessageReaderExtensions
{
    static bool IsAsyncResponse(BackendCode code) => code is BackendCode.NoticeResponse or BackendCode.NotificationResponse or BackendCode.ParameterStatus;

    /// <summary>
    /// Skip messages until code does not equal Current.Code.
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="code"></param>
    /// <param name="status">returned if we found an async response or couldn't skip past 'code' yet.</param>
    /// <returns>Returns true if skip succeeded, false if it could not skip past 'code' yet.</returns>
    public static bool SkipSimilar(this ref MessageReader reader, BackendCode code, out ReadStatus status)
    {
        bool moved = true;
        while (reader.Current.Code == code && (moved = reader.MoveNext()))
        {}

        if (moved && IsAsyncResponse(reader.Current.Code))
        {
            status = ReadStatus.AsyncResponse;
            return false;
        }
        status = ReadStatus.NeedMoreData;
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
            status = IsAsyncResponse(code) ? ReadStatus.AsyncResponse : ReadStatus.InvalidData;
            return false;
        }

        if (ensureBuffered && reader.Reader.Remaining < reader.Current.PayloadLength)
        {
            status = ReadStatus.NeedMoreData;
            return false;
        }

        status = default;
        return true;
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

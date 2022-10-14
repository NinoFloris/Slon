using System;
using System.Text;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines.Protocol;

readonly record struct MessageHeader
{
    public const int ByteCount = sizeof(uint) + sizeof(BackendCode);

    readonly uint _length;
    readonly BackendCode _code;

    public MessageHeader(BackendCode code, uint length)
    {
        _code = code;
        _length = length;
    }

    public BackendCode Code => _code;
    public uint Length => _length;

    public bool IsDefault => _code == 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryParse(ReadOnlySpan<byte> span, out MessageHeader header)
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

        header = new MessageHeader(code, length);
        return true;

        static void ThrowIfNotDefined(BackendCode code)
        {
            if (!EnumShim.IsDefined(code))
                throw new InvalidDataException("Unknown backend code: " + code);
        }
    }

    public static bool TryParse(in ReadOnlySequence<byte> buffer, out MessageHeader header)
    {
        Span<byte> span = stackalloc byte[ByteCount];
        if (!TryParse(buffer.GetFirstSpan(), out header) && (!ReadOnlySequenceExtensions.TryCopySlow(buffer, span) || !TryParse(span, out header)))
        {
            header = default;
            return false;
        }

        return true;
    }

    public static bool TryParse(ref SequenceReader<byte> reader, out MessageHeader header)
    {
        Span<byte> span = stackalloc byte[ByteCount];
        if (!TryParse(reader.UnreadSpan, out header) && (!ReadOnlySequenceExtensions.TryCopySlow(reader.Sequence.Slice(reader.Position), span) || !TryParse(span, out header)))
        {
            header = default;
            return false;
        }

        reader.Advance(ByteCount);
        return true;
    }
}

// Could be optimized like SequencePosition where the flags are in the top bits of the integer.
readonly struct MessageStartOffset
{
    [Flags]
    enum MessageStartOffsetFlags: byte
    {
        None = 0,
        ResumptionOffset = 1,
        BeforeFirstMove = 2
    }

    readonly long _offset;
    readonly MessageStartOffsetFlags _flags;

    MessageStartOffset(long offset, MessageStartOffsetFlags flags)
        : this(offset)
    {
        _flags = flags;
    }
    public MessageStartOffset(long offset) => _offset = offset;

    public bool IsResumption => (_flags & MessageStartOffsetFlags.ResumptionOffset) != 0;
    public bool IsBeforeFirstMove => (_flags & MessageStartOffsetFlags.BeforeFirstMove) != 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long GetOffset() => (_flags & MessageStartOffsetFlags.ResumptionOffset) != 0 ? -_offset : _offset;

    public static MessageStartOffset Recreate(long offset) => new(offset, MessageStartOffsetFlags.BeforeFirstMove);
    public static MessageStartOffset Resume(long offset) => new(offset, MessageStartOffsetFlags.ResumptionOffset | MessageStartOffsetFlags.BeforeFirstMove);
    public MessageStartOffset ToFirstMove() => new(_offset, _flags & ~MessageStartOffsetFlags.BeforeFirstMove);
}

[DebuggerDisplay("Code = {_current.Code}, Length = {_current.Length}")]
ref struct MessageReader
{
    SequenceReader<byte> _reader;
    MessageStartOffset _currentStart;
    MessageHeader _current;

    MessageReader(ReadOnlySequence<byte> sequence) => _reader = new SequenceReader<byte>(sequence);
    MessageReader(ReadOnlySequence<byte> sequence, MessageHeader current, MessageStartOffset currentStart)
        : this(sequence)
    {
        _current = current;
        _currentStart = currentStart;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    readonly long GetUnvalidatedCurrentConsumed() => _reader.Consumed - _currentStart.GetOffset();

    public readonly ReadOnlySequence<byte> Sequence => _reader.Sequence;
    public readonly ReadOnlySequence<byte> UnconsumedSequence => _reader.Sequence.Slice(_reader.Position);
    public readonly long Consumed => _reader.Consumed;

    public bool HasCurrent => !_current.IsDefault;

    public readonly BackendCode CurrentCode
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            if (_current.IsDefault)
                ThrowEnumOpCantHappen();

            return _current.Code;
        }
    }

    public readonly MessageHeader Current
    {
        get
        {
            if (_current.IsDefault)
                ThrowEnumOpCantHappen();

            return _current;
        }
    }

    public readonly MessageStartOffset CurrentStartOffset
    {
        get
        {
            if (_current.IsDefault)
                ThrowEnumOpCantHappen();

            return _currentStart;
        }
    }

    public readonly uint CurrentConsumed
    {
        get
        {
            if (_current.IsDefault)
                ThrowEnumOpCantHappen();

            var consumed = GetUnvalidatedCurrentConsumed();
            if (consumed < 0)
                ThrowOutOfRange();

            return (uint)consumed;

            static void ThrowOutOfRange() => throw new IndexOutOfRangeException("The current message offset lies past what the reader has consumed.");
        }
    }

    public readonly uint CurrentRemaining
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            if (_current.IsDefault)
                ThrowEnumOpCantHappen();

            var remaining = _current.Length - GetUnvalidatedCurrentConsumed();
            if (remaining < 0)
                ThrowOutOfRange();

            return (uint)remaining;

            static void ThrowOutOfRange() => throw new IndexOutOfRangeException("The reader has consumed past the end of the current message.");
        }
    }

    public readonly bool IsCurrentBuffered => _reader.Remaining >= CurrentRemaining;

    public static MessageReader Create(scoped in ReadOnlySequence<byte> sequence) => new(sequence);

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
            ThrowArgumentOutOfRangeException();

        var reader = new MessageReader(sequence, resumptionData.Header, MessageStartOffset.Recreate((uint)(consumed - resumptionData.MessageIndex)));
        reader._reader.Advance(consumed);
        return reader;

        static void ThrowArgumentOutOfRangeException() =>
            throw new ArgumentOutOfRangeException(nameof(resumptionData), "Resumption data carries an invalid MessageIndex for the given consumed value, as its larger than the value of consumed.");
    }

    /// <summary>
    /// Create a reader over a new sequence, one that starts after the consumed point of the previous one.
    /// </summary>
    /// <param name="sequence"></param>
    /// <param name="resumptionData"></param>
    /// <returns></returns>
    public static MessageReader Resume(scoped in ReadOnlySequence<byte> sequence, scoped in ResumptionData resumptionData) =>
        new(sequence, resumptionData.Header, MessageStartOffset.Resume(resumptionData.MessageIndex));

    public readonly ResumptionData GetResumptionData() => _current.IsDefault ? default : new(_current, (uint)GetUnvalidatedCurrentConsumed());

    public bool MoveNext()
    {
        if (_currentStart.IsBeforeFirstMove)
        {
            _currentStart = _currentStart.ToFirstMove();
            return true;
        }

        if (!_current.IsDefault && !ConsumeCurrent())
            return false;

        _currentStart = new MessageStartOffset(_reader.Consumed);
        if (!MessageHeader.TryParse(ref _reader, out _current))
        {
            // We couldn't fully parse the next header, reset all 'current' state, specifically important for resumption data.
            _current = default;
            _currentStart = default;
            return false;
        }

        return true;
    }

    /// <summary>
    /// Consume always succeeds if Current is buffered and never if it isn't.
    /// </summary>
    /// <returns>A bool signalling whether the reader advanced past the current message, this may leave an empty reader.</returns>
    public bool ConsumeCurrent()
    {
        if (_currentStart.IsBeforeFirstMove)
            ThrowEnumOpCantHappen();

        var remaining = CurrentRemaining;
        if (remaining == 0)
            return true;

        if (_reader.Remaining < remaining)
            return false;

        _reader.Advance(remaining);
        return true;
    }

    public bool TryReadCString([NotNullWhen(true)]out string? value)
    {
        if (_reader.TryReadTo(out ReadOnlySequence<byte> strBytes, 0))
        {
            value = PgEncoding.RelaxedUTF8.GetString(strBytes);
            return true;
        }

        value = null;
        return false;
    }

    public bool TryReadCStringBuffer(out ReadOnlySequence<byte> strBytes) => _reader.TryReadTo(out strBytes, 0);

    public bool TryCopyTo(Span<byte> destination) => _reader.TryCopyTo(destination);
    public bool TryReadShort(out short value) => _reader.TryReadBigEndian(out value);
    public bool TryReadInt(out int value) => _reader.TryReadBigEndian(out value);
    public bool TryReadByte(out byte value) => _reader.TryRead(out value);
    public void Advance(long count) => _reader.Advance(count);

    public void Rewind(long count)
    {
        // https://github.com/dotnet/runtime/issues/68774
        if (count == 0) return;
        _reader.Rewind(count);
    }

    public readonly record struct ResumptionData(
        // The current message at the time of the suspend.
        MessageHeader Header,
        // Where we need to start relative to the message in Header.
        uint MessageIndex
    )
    {
        public bool IsDefault => Header.IsDefault;
    }

    [DoesNotReturn]
    static void ThrowEnumOpCantHappen() => throw new InvalidOperationException("Enumeration has either not started or has already finished.");
}

static class MessageReaderExtensions
{
    public static bool IsAsyncResponse(this BackendCode code) => code is BackendCode.NoticeResponse or BackendCode.NotificationResponse or BackendCode.ParameterStatus;

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
        while (reader.CurrentCode == code && (moved = reader.MoveNext()))
        {}

        if (moved && reader.CurrentCode.IsAsyncResponse())
        {
            status = ReadStatus.AsyncResponse;
            return false;
        }
        status = ReadStatus.NeedMoreData;
        return moved;
    }

    public static bool TryReadUInt(this ref MessageReader reader, out uint value)
    {
        UnsafeShim.SkipInit(out value);
        return reader.TryReadInt(out Unsafe.As<uint, int>(ref value));
    }

    public static bool TryReadBool(this ref MessageReader reader, out bool value)
    {
        if (!reader.TryReadByte(out var b))
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
        if (reader.CurrentCode != code)
        {
            status = code.IsAsyncResponse() ? ReadStatus.AsyncResponse : ReadStatus.InvalidData;
            return false;
        }

        if (ensureBuffered && !reader.IsCurrentBuffered)
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

    public static bool ReadMessage<T>(this ref MessageReader reader, out ReadStatus status) where T : struct, IBackendMessage
    {
        status = new T().Read(ref reader);
        return status == ReadStatus.Done;
    }

    public static bool ReadMessage<T>(this ref MessageReader reader, T message, out ReadStatus status) where T : IBackendMessage
    {
        status = message.Read(ref reader);
        return status == ReadStatus.Done;
    }
}

using System;
using System.Text;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Npgsql.Pipelines.Protocol;

interface IHeader<THeader>: IEquatable<THeader> where THeader: struct, IHeader<THeader>
{
    /// <summary>
    /// Number of bytes the header consists of.
    /// </summary>
    int HeaderLength { get; }
    /// <summary>
    /// Length of the entire message, including the header.
    /// </summary>
    long Length { get; }
    /// <summary>
    /// Whether the current instance is the default for this type.
    /// </summary>
    bool IsDefault { get; }
    /// <summary>
    /// Whether this message should be handled by the protocol.
    /// </summary>
    bool IsAsyncResponse { get; }

    // Too bad static abstract interface methods are not supported on ns2.0.
    bool TryParse(in ReadOnlySpan<byte> unreadSpan, in ReadOnlySequence<byte> buffer, long bufferStart, out THeader header);

    /// <summary>
    /// Header equality up to type (packet code, flags, whatever is enough to distinguish *types of messages*).
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    bool TypeEquals(in THeader other);
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

[DebuggerDisplay("Current = {_current}")]
ref struct MessageReader<THeader> where THeader : struct, IHeader<THeader>, IEquatable<THeader>
{
    SequenceReader<byte> _reader;
    MessageStartOffset _currentStart;
    THeader _current;

    MessageReader(ReadOnlySequence<byte> sequence) => _reader = new SequenceReader<byte>(sequence);
    MessageReader(ReadOnlySequence<byte> sequence, THeader current, MessageStartOffset currentStart)
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
    public readonly long Remaining => _reader.Remaining;

    public bool HasCurrent => !_current.IsDefault;

    [UnscopedRef]
    public readonly ref readonly THeader Current
    {
        get
        {
            if (_current.IsDefault)
                ThrowEnumOpCantHappen();

            return ref _current;
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

    public bool CurrentIsAsyncResponse
    {
        get
        {
            if (_current.IsDefault)
                ThrowEnumOpCantHappen();

            return _current.IsAsyncResponse;
        }
    }

    public readonly bool CurrentIsBuffered => _reader.Remaining >= CurrentRemaining;

    public static MessageReader<THeader> Create(scoped in ReadOnlySequence<byte> sequence) => new(sequence);
    public static MessageReader<THeader> Create(scoped in ReadOnlySequence<byte> sequence, long consumed)
    {
        var reader = new MessageReader<THeader>(sequence);
        reader.Advance(consumed);
        return reader;
    }

    /// <summary>
    /// Recreate a reader over the same (or the same starting point) sequence with resumption data.
    /// </summary>
    /// <param name="sequence"></param>
    /// <param name="resumptionData"></param>
    /// <param name="consumed"></param>
    /// <returns></returns>
    public static MessageReader<THeader> Recreate(scoped in ReadOnlySequence<byte> sequence, scoped in ResumptionData resumptionData, long consumed)
    {
        if (consumed - resumptionData.MessageIndex < 0)
            ThrowArgumentOutOfRangeException();

        var currentStart = MessageStartOffset.Recreate((uint)(consumed - resumptionData.MessageIndex));
        var reader = new MessageReader<THeader>(sequence, resumptionData.Header, resumptionData.IsDefault ? currentStart.ToFirstMove() : currentStart);
        reader._reader.Advance(consumed);

        // If the header was unknown at the time of yielding we'll move here to bring the new header into view.
        // This mainly allows us to 'goto' in messages where the actual resumption point would be a nested message inside a switch case.
        // On resumption the switch expects reader.Current.Code to have some value (as you can't 'goto' a specific case directly from the outside).
        // There are ways around that but effectively this makes writing straight line code more easily correct, which is paramount.
        if (resumptionData.IsDefault)
        {
            if (!reader.MoveNext())
                ThrowCouldNotMoveNext();

            // Re-arm BeforeFirstMove.
            reader._currentStart = currentStart;
        }
        return reader;

        static void ThrowCouldNotMoveNext() =>
            throw new InvalidOperationException("Could not do MoveNext after receiving a default resumption data, assumption is that there is enough new buffer to read at least a header after a resumption.");

        static void ThrowArgumentOutOfRangeException() =>
            throw new ArgumentOutOfRangeException(nameof(resumptionData), "Resumption data carries an invalid MessageIndex for the given consumed value, as its larger than the value of consumed.");
    }

    /// <summary>
    /// Create a reader over a new sequence, one that starts after the consumed point of the previous one.
    /// </summary>
    /// <param name="sequence"></param>
    /// <param name="resumptionData"></param>
    /// <returns></returns>
    public static MessageReader<THeader> Resume(scoped in ReadOnlySequence<byte> sequence, scoped in ResumptionData resumptionData)
    {
        var currentStart = MessageStartOffset.Resume(resumptionData.MessageIndex);
        var reader = new MessageReader<THeader>(sequence, resumptionData.Header, resumptionData.IsDefault ? currentStart.ToFirstMove() : currentStart);

        // If the header was unknown at the time of yielding we'll move here to bring the new header into view.
        // This mainly allows us to 'goto' in messages where the actual resumption point would be a nested message inside a switch case.
        // On resumption the switch expects reader.Current.Code to have some value (as you can't 'goto' a specific case directly from the outside).
        // There are ways around that but effectively this makes writing straight line code more easily correct, which is paramount.
        if (resumptionData.IsDefault)
        {
            if (!reader.MoveNext())
                ThrowCouldNotMoveNext();

            // Re-arm BeforeFirstMove.
            reader._currentStart = currentStart;
        }

        return reader;

        static void ThrowCouldNotMoveNext() =>
            throw new InvalidOperationException("Could not do MoveNext after receiving a default resumption data, assumption is that there is enough new buffer to read at least a header after a resumption.");
    }

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
        if (!default(THeader).TryParse(_reader.UnreadSpan, _reader.Sequence, _reader.Consumed, out _current))
        {
            // We couldn't fully parse the next header, reset all 'current' state, specifically important for resumption data.
            _current = default;
            _currentStart = default;
            return false;
        }

        _reader.Advance(_current.HeaderLength);

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

    public bool CurrentEquals(scoped in THeader other)
    {
        if (_current.IsDefault)
            ThrowEnumOpCantHappen();

        return _current.Equals(other);
    }

    public bool CurrentTypeEquals(scoped in THeader other)
    {
        if (_current.IsDefault)
            ThrowEnumOpCantHappen();

        return _current.TypeEquals(other);
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
        THeader Header,
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
    /// <summary>
    /// Skip messages until header does not equal TypeEquals.
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="code"></param>
    /// <param name="matchingHeader"></param>
    /// <param name="status">returned if we found an async response or couldn't skip past 'code' yet.</param>
    /// <returns>Returns true if skip succeeded, false if it could not skip past 'code' yet.</returns>
    public static bool SkipSimilar<THeader>(this ref MessageReader<THeader> reader, scoped in THeader matchingHeader, out ReadStatus status)
        where THeader : struct, IHeader<THeader>
    {
        bool moved = true;
        while (reader.CurrentTypeEquals(matchingHeader) && (moved = reader.MoveNext()))
        {}

        if (moved && reader.CurrentIsAsyncResponse)
        {
            status = ReadStatus.AsyncResponse;
            return false;
        }
        status = moved ? ReadStatus.Done : ReadStatus.NeedMoreData;
        return moved;
    }

    public static bool TryReadUInt<THeader>(this ref MessageReader<THeader> reader, out uint value)
        where THeader : struct, IHeader<THeader>
    {
        UnsafeShim.SkipInit(out value);
        return reader.TryReadInt(out Unsafe.As<uint, int>(ref value));
    }

    public static bool TryReadUShort<THeader>(this ref MessageReader<THeader> reader, out ushort value)
        where THeader : struct, IHeader<THeader>
    {
        UnsafeShim.SkipInit(out value);
        return reader.TryReadShort(out Unsafe.As<ushort, short>(ref value));
    }

    public static bool TryReadBool<THeader>(this ref MessageReader<THeader> reader, out bool value)
        where THeader : struct, IHeader<THeader>
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
    public static bool IsExpected<THeader>(this ref MessageReader<THeader> reader, scoped in THeader matchingHeader, out ReadStatus status, bool ensureBuffered = false) 
        where THeader : struct, IHeader<THeader>
    {
        if (!reader.CurrentTypeEquals(matchingHeader))
        {
            status = matchingHeader.IsAsyncResponse ? ReadStatus.AsyncResponse : ReadStatus.InvalidData;
            return false;
        }

        if (ensureBuffered && !reader.CurrentIsBuffered)
        {
            status = ReadStatus.NeedMoreData;
            return false;
        }

        status = ReadStatus.Done;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool MoveNextAndIsExpected<THeader>(this ref MessageReader<THeader> reader, scoped in THeader matchingHeader, out ReadStatus status, bool ensureBuffered = false) 
        where THeader : struct, IHeader<THeader>
    {
        if (!reader.MoveNext())
        {
            status = ReadStatus.NeedMoreData;
            return false;
        }

        return reader.IsExpected(matchingHeader, out status, ensureBuffered);
    }

    public static bool ReadMessage<THeader, T>(this ref MessageReader<THeader> reader, scoped ref T message, out ReadStatus status)
        where T : IBackendMessage<THeader> where THeader : struct, IHeader<THeader>
    {
        status = message.Read(ref reader);
        return status == ReadStatus.Done;
    }
}

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Npgsql.Pipelines;

/// <summary>
/// Buffer writer which backfills a PG header on copy.
/// </summary>
class HeaderBufferWriter: ICopyableBufferWriter<byte>
{
    readonly int _minimumSegmentSize;
    int _bytesWritten;

    List<CompletedBuffer>? _completedSegments;
    byte[]? _currentSegment;
    int _position;

    public HeaderBufferWriter(int minimumSegmentSize = 4096)
    {
        _minimumSegmentSize = minimumSegmentSize;
        _bytesWritten = MessageHeader.CodeAndLengthByteCount;
        _position = MessageHeader.CodeAndLengthByteCount;
    }

    byte _code;

    public void SetCode(byte value) => _code = value;

    public void Reset()
    {
        if (_completedSegments != null)
        {
            for (var i = 0; i < _completedSegments.Count; i++)
            {
                _completedSegments[i].Return();
            }

            _completedSegments.Clear();
        }

        if (_currentSegment != null)
        {
            ArrayPool<byte>.Shared.Return(_currentSegment);
            _currentSegment = null;
        }

        _bytesWritten = MessageHeader.CodeAndLengthByteCount;
        _position = MessageHeader.CodeAndLengthByteCount;
        _code = 0;
    }

    public void Advance(int count)
    {
        _bytesWritten += count;
        _position += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);

        return _currentSegment.AsMemory(_position, _currentSegment.Length - _position);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);

        return _currentSegment.AsSpan(_position, _currentSegment.Length - _position);
    }

    public void CopyTo(IBufferWriter<byte> destination)
    {
        WriteHeader();
        if (_completedSegments != null)
        {
            // Copy completed segments
            var count = _completedSegments.Count;
            for (var i = 0; i < count; i++)
            {
                destination.Write(_completedSegments[i].Span);
            }
        }

        destination.Write(_currentSegment.AsSpan(0, _position));
    }

    [MemberNotNull(nameof(_currentSegment))]
    void EnsureCapacity(int sizeHint)
    {
        // This does the Right Thing. It only subtracts _position from the current segment length if it's non-null.
        // If _currentSegment is null, it returns 0.
        var remainingSize = _currentSegment?.Length - _position ?? 0;

        // If the sizeHint is 0, any capacity will do
        // Otherwise, the buffer must have enough space for the entire size hint, or we need to add a segment.
        if ((sizeHint == 0 && remainingSize > 0) || (sizeHint > 0 && remainingSize >= sizeHint))
        {
            // We have capacity in the current segment
#pragma warning disable CS8774 // Member must have a non-null value when exiting.
            return;
#pragma warning restore CS8774 // Member must have a non-null value when exiting.
        }

        AddSegment(sizeHint);
    }

    [MemberNotNull(nameof(_currentSegment))]
    void AddSegment(int sizeHint = 0)
    {
        if (_currentSegment != null)
        {
            // We're adding a segment to the list
            if (_completedSegments == null)
            {
                _completedSegments = new List<CompletedBuffer>();
            }

            // Position might be less than the segment length if there wasn't enough space to satisfy the sizeHint when
            // GetMemory was called. In that case we'll take the current segment and call it "completed", but need to
            // ignore any empty space in it.
            _completedSegments.Add(new CompletedBuffer(_currentSegment, _position));
        }

        if (_currentSegment != null)
            _position = 0;

        // Get a new buffer using the minimum segment size, unless the size hint is larger than a single segment.
        _currentSegment = ArrayPool<byte>.Shared.Rent(Math.Max(_minimumSegmentSize, sizeHint));
    }

    void WriteHeader()
    {
        var segment = _completedSegments == null ? _currentSegment : _completedSegments[0].Buffer;

        if (segment is not null)
        {
            var span = segment.AsSpan();
            if (_code == 0)
                throw new InvalidOperationException("Missing code, code should be set before copying out data.");
            span[0] = _code;
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(1), _bytesWritten - (MessageHeader.CodeAndLengthByteCount - MessageWriter.IntByteCount));
        }

    }

    public WrittenBuffers DetachAndReset()
    {
        _completedSegments ??= new List<CompletedBuffer>();

        if (_currentSegment is not null)
        {
            _completedSegments.Add(new CompletedBuffer(_currentSegment, _position));
        }

        var written = new WrittenBuffers(_completedSegments, _bytesWritten);

        _currentSegment = null;
        _completedSegments = null;
        _bytesWritten = 0;
        _position = 0;

        return written;
    }

    /// <summary>
    /// Holds the written segments from a MemoryBufferWriter and is no longer attached to a MemoryBufferWriter.
    /// You are now responsible for calling Dispose on this type to return the memory to the pool.
    /// </summary>
    internal readonly ref struct WrittenBuffers
    {
        public readonly List<CompletedBuffer> Segments;
        readonly int _bytesWritten;

        public WrittenBuffers(List<CompletedBuffer> segments, int bytesWritten)
        {
            Segments = segments;
            _bytesWritten = bytesWritten;
        }

        public int ByteLength => _bytesWritten;

        public void Dispose()
        {
            for (var i = 0; i < Segments.Count; i++)
            {
                Segments[i].Return();
            }
            Segments.Clear();
        }
    }

    /// <summary>
    /// Holds a byte[] from the pool and a size value. Basically a Memory but guaranteed to be backed by an ArrayPool byte[], so that we know we can return it.
    /// </summary>
    internal readonly struct CompletedBuffer
    {
        public byte[] Buffer { get; }
        public int Length { get; }

        public ReadOnlySpan<byte> Span => Buffer.AsSpan(0, Length);

        public CompletedBuffer(byte[] buffer, int length)
        {
            Buffer = buffer;
            Length = length;
        }

        public void Return()
        {
            ArrayPool<byte>.Shared.Return(Buffer);
        }
    }
}

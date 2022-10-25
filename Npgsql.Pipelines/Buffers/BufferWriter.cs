// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace System.Buffers;

/// <summary>
/// A fast access struct that wraps <see cref="IBufferWriter{T}"/>.
/// </summary>
/// <typeparam name="T">The type of element to be written.</typeparam>
internal struct BufferWriter<T> : IBufferWriter<byte> where T : IBufferWriter<byte>
{
    /// <summary>
    /// The underlying <see cref="IBufferWriter{T}"/>.
    /// </summary>
    private readonly T _output;

    /// <summary>
    /// The result of the last call to <see cref="IBufferWriter{T}.GetMemory(int)"/>, less any bytes already "consumed" with <see cref="Advance(int)"/>.
    /// Backing field for the <see cref="Span"/> property.
    /// </summary>
    private Memory<byte> _memory;

    /// <summary>
    /// The number of uncommitted bytes (all the calls to <see cref="Advance(int)"/> since the last call to <see cref="Commit"/>).
    /// </summary>
    public int BufferedBytes { get; private set; }

    /// <summary>
    /// The total number of bytes written with this writer.
    /// Backing field for the <see cref="BytesCommitted"/> property.
    /// </summary>
    private long _bytesCommitted;

    /// <summary>
    /// Initializes a new instance of the <see cref="BufferWriter{T}"/> struct.
    /// </summary>
    /// <param name="output">The <see cref="IBufferWriter{T}"/> to be wrapped.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BufferWriter(T output)
    {
        BufferedBytes = 0;
        _bytesCommitted = 0;
        _output = output;
        _memory = output.GetMemory();
    }

    public readonly T Output => _output;

    /// <summary>
    /// Gets the result of the last call to <see cref="IBufferWriter{T}.GetSpan(int)"/>.
    /// </summary>
    public readonly Span<byte> Span => _memory.Span.Slice(BufferedBytes);

    /// <summary>
    /// Gets the result of the last call to <see cref="IBufferWriter{T}.GetSpan(int)"/>.
    /// </summary>
    public readonly Memory<byte> Memory => _memory.Slice(BufferedBytes);

    /// <summary>
    /// Gets the total number of bytes written with this writer.
    /// </summary>
    public readonly long BytesCommitted => _bytesCommitted;

    /// <summary>
    /// Calls <see cref="IBufferWriter{T}.Advance(int)"/> on the underlying writer
    /// with the number of uncommitted bytes.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Commit()
    {
        var buffered = BufferedBytes;
        if (buffered > 0)
        {
            _output.Advance(buffered);
            _bytesCommitted += buffered;
            BufferedBytes = 0;
            _memory = _output.GetMemory();
        }
    }

    /// <summary>
    /// Used to indicate that part of the buffer has been written to.
    /// </summary>
    /// <param name="count">The number of bytes written to.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(int count)
    {
        BufferedBytes += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return Memory;
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return Span;
    }

    /// <summary>
    /// Copies the caller's buffer into this writer and calls <see cref="Advance(int)"/> with the length of the source buffer.
    /// </summary>
    /// <param name="source">The buffer to copy in.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ReadOnlySpan<byte> source)
    {
        if (_memory.Length >= source.Length)
        {
            source.CopyTo(_memory.Span);
            Advance(source.Length);
        }
        else
        {
            WriteMultiBuffer(source);
        }
    }

    /// <summary>
    /// Acquires a new buffer if necessary to ensure that some given number of bytes can be written to a single buffer.
    /// </summary>
    /// <param name="count">The number of bytes that must be allocated in a single buffer.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Ensure(int count = 1)
    {
        if (_memory.Length - BufferedBytes < count)
        {
            EnsureMore(count);
        }
    }

    /// <summary>
    /// Gets a fresh span to write to, with an optional minimum size.
    /// </summary>
    /// <param name="count">The minimum size for the next requested buffer.</param>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void EnsureMore(int count = 0)
    {
        if (BufferedBytes > 0)
        {
            Commit();
        }

        _memory = _output.GetMemory(count);
    }

    /// <summary>
    /// Copies the caller's buffer into this writer, potentially across multiple buffers from the underlying writer.
    /// </summary>
    /// <param name="source">The buffer to copy into this writer.</param>
    private void WriteMultiBuffer(ReadOnlySpan<byte> source)
    {
        while (source.Length > 0)
        {
            if (_memory.Length == 0)
            {
                EnsureMore();
            }

            var writable = Math.Min(source.Length, _memory.Length);
            source.Slice(0, writable).CopyTo(_memory.Span);
            source = source.Slice(writable);
            Advance(writable);
        }
    }
}

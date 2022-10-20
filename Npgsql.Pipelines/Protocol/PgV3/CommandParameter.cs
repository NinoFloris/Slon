using System;
using System.Buffers;
using System.Collections.Generic;
using Microsoft.AspNetCore.Internal;

namespace Npgsql.Pipelines.Protocol;

struct Oid
{
    public Oid(int oid) => Value = oid;

    public int Value { get; }
    public static Oid Unknown => new(0);
}

enum FormatCode: short
{
    Text = 0,
    Binary = 1
}

interface IParameterWriter
{
    bool TryPrecomputeSize(FormatCode code, object value, out int size);
    void Write<T>(ref BufferWriter<T> writer, FormatCode code, object value) where T : IBufferWriter<byte>;
}

class BlittingWriter: IParameterWriter
{
    public static BlittingWriter Instance { get; } = new();

    public bool TryPrecomputeSize(FormatCode code, object value, out int size)
        => throw new NotSupportedException("BlittingWriter is only a writer.");

    public void Write<T>(ref BufferWriter<T> writer, FormatCode code, object value) where T : IBufferWriter<byte>
    {
        if (value is not MemoryBufferWriter.WrittenBuffers buf)
            throw new InvalidOperationException("Unexpected value of type: " + value.GetType().FullName);

        writer.WriteInt(MessageWriter.IntByteCount + buf.ByteLength);
        foreach (var bufSegment in buf.Segments)
        {
            writer.WriteRaw(bufSegment.Span);
            bufSegment.Return();
        }
    }
}

class DBNullWriter : IParameterWriter
{
    public bool TryPrecomputeSize(FormatCode code, object value, out int size)
    {
        if (value is DBNull)
        {
            size = 0;
            return true;
        }

        size = 0;
        return false;
    }

    public void Write<T>(ref BufferWriter<T> writer, FormatCode code, object value) where T : IBufferWriter<byte>
    {
        writer.WriteInt(-1);
    }
}

record struct CommandParameter(Oid Oid, FormatCode FormatCode, int Length, object Value);

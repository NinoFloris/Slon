using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
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

public class DbParameter: System.Data.Common.DbParameter
{
    public override void ResetDbType()
    {
        throw new NotImplementedException();
    }

    public override DbType DbType { get; set; }
    public override ParameterDirection Direction { get; set; }
    public override bool IsNullable { get; set; }
    [AllowNull]
    public override string ParameterName { get; set; }
    [AllowNull]
    public override string SourceColumn { get; set; }
    [AllowNull]
    public override object Value { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }


    internal FormatCode FormatCode { get; }
    internal Oid Oid { get; }
}

public interface IStrongBox<T>
{
    T Value { get; }
}

class Test
{
    public KeyValuePair<CommandParameter, IParameterWriter> MapParameter(DbParameter parameter)
    {
        var parameterWriter = ResolveParameterWriter(parameter);
        if (parameterWriter.TryPrecomputeSize(parameter.FormatCode, parameter.Value, out var size))
        {
            var p = new CommandParameter(parameter.Oid, parameter.FormatCode, size, parameter.Value);
            return new KeyValuePair<CommandParameter, IParameterWriter>(p, parameterWriter);
        }
        else
        {
            // If we couldn't precompute the size we are going to write it out directly, taking the length, and cache the buffers until Bind.
            // This happens when it's more expensive to calculate the length than it would be to cache the result, for instance, json serialization.
            var writer = new BufferWriter<MemoryBufferWriter>(MemoryBufferWriter.Get());
            try
            {
                parameterWriter.Write(ref writer, parameter.FormatCode, parameter.Value);
                var p = new CommandParameter(parameter.Oid, parameter.FormatCode, (int)writer.BytesCommitted, writer.Output.DetachAndReset());
                return new KeyValuePair<CommandParameter, IParameterWriter>(p, BlittingWriter.Instance);
            }
            finally
            {
                MemoryBufferWriter.Return(writer.Output);
            }
        }
    }

    IParameterWriter ResolveParameterWriter(DbParameter p)
    {
        // Do all the fun db type lookup etc.

        return default!;
    }
}

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Slon.Buffers;
using Slon.Pg;
using Slon.Protocol.Pg;

namespace Slon.Protocol.PgV3;

readonly struct ParametersWriter: IDisposable
{
    readonly PgV3Protocol _protocol;
    readonly PgWriter _pgWriter;
    readonly int _minimumBufferSegmentSize;

    public ParametersWriter(ParameterContext context, PgV3Protocol protocol, PgTypeCatalog typeCatalog, FlushMode flushMode)
    {
        _protocol = protocol;
        _pgWriter = context.Parameters.IsEmpty ? null! : protocol.RentPgWriter(flushMode, typeCatalog);
        _minimumBufferSegmentSize = context.MinimalBufferSegmentSize ?? -1;
        Context = context;
    }

    public ParameterContext Context { get; }
    public PooledMemory<Parameter> Items => Context.Parameters;

    /// <summary>
    /// 
    /// </summary>
    /// <param name="byteCount"></param>
    /// <returns>False when there are only exact and upper bound byte count parameters, if small enough, these can use one pass writing.</returns>
    public bool TryGetTotalByteCount(out int byteCount)
    {
        if (Context.Parameters.IsEmpty)
        {
            byteCount = 0;
            return true;
        }

        if (Context.HasUpperBoundByteCounts())
        {
            byteCount = default;
            return false;
        }

        var unknownCount = 0;
        if (Context.HasUnknownByteCounts())
            foreach (var p in Items.Span)
                if (p.Size?.Kind is SizeResultKind.Unknown)
                    unknownCount++;

        var unknownOutputs = unknownCount > 0 ? ArrayPool<BufferedOutput>.Shared.Rent(unknownCount) : null!;
        var unknownI = -1;

        var totalByteCount = 0;
        foreach (var p in Items.Span)
            if (p.Size?.Kind is SizeResultKind.Unknown)
                unknownOutputs[unknownI++] = p.GetBufferedOutput();
            else
                totalByteCount += p.Size?.Value ?? 0;

        // TODO If byteCount small enough return false to do one pass writing.
        byteCount = totalByteCount + (Items.Length * 4);
        if (unknownOutputs is null)
            _pgWriter.UpdateState(unknownOutputs, SizeResult.Create(byteCount));
        else
            _pgWriter.UpdateState(unknownOutputs, unknownOutputs.Length == 0 ? SizeResult.Zero : SizeResult.Unknown);

        return true;
    }

    public int WriteOnePass<TWriter>(ref BufferWriter<TWriter> buffer) where TWriter : IBufferWriter<byte>
    {
        Debug.Assert(_minimumBufferSegmentSize >= buffer.Span.Length);

        var pgWriter = _pgWriter;
        pgWriter.SuppressFlushes();
        try
        {
            var bytesCommitted = buffer.BytesCommitted;
            var initialBytesBuffered = buffer.BufferedBytes;
            foreach (var p in Items.Span)
            {
                var bytesBuffered = buffer.BufferedBytes;
                var size = p.Size;
                WriteParameter(ref buffer, pgWriter, p, size, null, onePass: true);

                // Converter went over its previously given byte count here, fail loudly as this is an implementation issue in the converter.
                if (bytesCommitted != buffer.BytesCommitted || buffer.BufferedBytes - bytesBuffered > sizeof(int) + size.GetValueOrDefault().Value.GetValueOrDefault())
                    throw new InvalidOperationException($"The '{p.GetConverterType().FullName}' converter wrote more than the previously reported size of the value.");
            }

            return buffer.BufferedBytes - initialBytesBuffered;
        }
        finally
        {
            pgWriter.RestoreFlushes();
        }
    }

    public void Write<TWriter>(ref BufferWriter<TWriter> buffer) where TWriter : IBufferWriter<byte>
    {
        var pgWriter = _pgWriter;
        buffer.Commit();
        pgWriter.RestartWriter();
        var unknownI = -1;
        var unknownOutputs = (BufferedOutput[])_pgWriter.State!;
        for (var i = 0; i < Items.Span.Length; i++)
        {
            var p = Items.Span[i];
            var bytesBuffered = buffer.BufferedBytes;
            var size = p.Size;
            var bufferedOutput = size is { Kind: SizeResultKind.Unknown } ? new BufferedOutput?(unknownOutputs[unknownI++]) : null;
            WriteParameter(ref buffer, pgWriter, p, size, bufferedOutput, onePass: false);

            // Converter went over its previously given byte count here, fail loudly as this is an implementation issue in the converter.
            if (size is { Kind: not SizeResultKind.Unknown, Value: { } byteCount } && buffer.BufferedBytes - bytesBuffered > sizeof(int) + byteCount)
                throw new InvalidOperationException($"The '{p.GetConverterType().FullName}' converter wrote more than the previously reported size of the value.");
        }
        pgWriter.Writer.Commit();
        buffer = new BufferWriter<TWriter>(buffer.Output);
    }

    static void WriteParameter<TWriter>(ref BufferWriter<TWriter> buffer, PgWriter pgWriter, Parameter p, SizeResult? sizeResult, BufferedOutput? bufferedOutput, bool onePass) where TWriter : IBufferWriter<byte>
    {
        Debug.Assert(onePass && pgWriter.FlushMode is FlushMode.None && bufferedOutput is null || !onePass);

        if (sizeResult is null)
        {
            buffer.WriteInt(-1); // NULL
            return;
        }

        if (!onePass && bufferedOutput is { } buffered)
        {
            buffer.WriteInt(buffered.Length);
            buffered.Write(pgWriter);
            return;
        }

        // We only need to consider upper bounds in one pass cases.
        if (onePass && p.Size?.Kind is SizeResultKind.UpperBound)
        {
            // Make a copy and advance so we can write the length in at the end.
            var backFillBuffer = buffer;
            buffer.Advance(sizeof(int));
            p.Write(pgWriter);
            var bufferedBytes = buffer.BufferedBytes - sizeof(int);
            backFillBuffer.WriteInt(bufferedBytes);
        }
        else if (sizeResult is {} size)
        {
            pgWriter.WriteInteger(size.Value.GetValueOrDefault());
            p.Write(pgWriter);
        }
    }

    public async ValueTask<FlushResult> WriteStreaming<TWriter>(MessageWriter<TWriter> writer, CancellationToken cancellationToken) where TWriter : IStreamingWriter<byte>
    {
        if (!MemoryMarshal.TryGetArray(Items.Memory, out var segment))
            return await WriteStreamingMemory(Items.Memory, writer, cancellationToken);

        var pgWriter = _pgWriter; // TODO init writer as flush ignored.
        var advisoryFlushThreshold = writer.AdvisoryFlushThreshold; // TODO measure, maybe we should only flush after every (auto) commit.
        var array = segment.Array!;
        for (var i = 0; i < segment.Count && i < array.Length; i++)
        {
            var bytesBuffered = writer.BufferedBytes;
            var p = array[i];
            await WriteParameter(writer, pgWriter, p, cancellationToken).ConfigureAwait(false);
            // Make sure we don't commit too often, as this requires a memory slice in the pipe
            // additionally any writer loop may start writing small packets if we let it know certain memory is returned.
            if (writer.BufferedBytes > advisoryFlushThreshold)
            {
                var result = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                if (result.IsCanceled || result.IsCompleted)
                    return result;
            }

            // Converter went over its previously given byte count here, fail loudly as this is an implementation issue in the converter.
            if (p.Size is { Kind: not SizeResultKind.Unknown, Value: { } byteCount } && writer.BufferedBytes - bytesBuffered > sizeof(int) + byteCount)
                throw new InvalidOperationException($"The '{p.GetConverterType().FullName}' converter wrote more than the previously reported size of the value.");
        }

        return new FlushResult();
    }

    // We have another copy of this code just for non-array memories, we don't split per parameter as that would need another async method + state machine etc.
    async ValueTask<FlushResult> WriteStreamingMemory<TWriter>(ReadOnlyMemory<Parameter> mem, MessageWriter<TWriter> writer, CancellationToken cancellationToken) where TWriter : IStreamingWriter<byte>
    {
        var advisoryFlushThreshold = writer.AdvisoryFlushThreshold; // TODO measure, maybe we should only flush after every (auto) commit.
        var pgWriter = _pgWriter;
        for (var i = 0; i < mem.Span.Length; i++)
        {
            var bytesBuffered = writer.BufferedBytes;
            var p = mem.Span[i];
            await WriteParameter(writer, pgWriter, p, cancellationToken).ConfigureAwait(false);
            // Make sure we don't commit too often, as this requires a memory slice in the pipe
            // additionally any writer loop may start writing small packets if we let it know certain memory is returned.
            if (writer.BufferedBytes > advisoryFlushThreshold)
            {
                var result = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                if (result.IsCanceled || result.IsCompleted)
                    return result;
            }

            // Converter went over its previously given byte count here, fail loudly as this is an implementation issue in the converter.
            if (p.Size is { Kind: not SizeResultKind.Unknown, Value: { } byteCount } && writer.BufferedBytes - bytesBuffered > sizeof(int) + byteCount)
                throw new InvalidOperationException($"The '{p.GetConverterType().FullName}' converter wrote more than the previously reported size of the value.");
        }

        return new FlushResult();
    }

    static ValueTask WriteParameter<TWriter>(MessageWriter<TWriter> writer, PgWriter pgWriter, Parameter p, CancellationToken cancellationToken) where TWriter : IStreamingWriter<byte>
    {
        if (p.IsDbNull)
        {
            writer.WriteInt(-1); // NULL
            return new ValueTask();
        }

        var sizeResult = p.Size.Value;
        switch (sizeResult.Kind)
        {
            case SizeResultKind.FixedSize:
            case SizeResultKind.Size:
                writer.WriteInt(sizeResult.Value.GetValueOrDefault());
                if (pgWriter.FlushMode is FlushMode.NonBlocking)
                    return p.WriteAsync(pgWriter, cancellationToken);

                p.Write(pgWriter);
                return new ValueTask();
            case SizeResultKind.Unknown:
                var buffer = p.GetBufferedOutput();
                writer.WriteInt(buffer.Length);
                if (pgWriter.FlushMode is FlushMode.NonBlocking)
                    return buffer.WriteAsync(pgWriter, cancellationToken);

                buffer.Write(pgWriter);
                return new ValueTask();
            case SizeResultKind.UpperBound:
                throw new NotSupportedException();
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public void Dispose()
    {
        if (_pgWriter is not null)
            _protocol.ReturnPgWriter(_pgWriter);
    }
}

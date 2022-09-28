using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines.QueryMessages;

readonly struct ResultColumnCodes
{
    ResultColumnCodes(FormatCode code) => OverallCode = code;
    ResultColumnCodes(ArraySegment<FormatCode> codes) => PerColumnCodes = codes;

    public bool IsOverallCode => PerColumnCodes.Array is null;
    public bool IsPerColumnCodes => PerColumnCodes.Array is not null;

    public FormatCode OverallCode { get; }
    public ArraySegment<FormatCode> PerColumnCodes { get; }

    public static ResultColumnCodes NoColumns => new(new ArraySegment<FormatCode>(Array.Empty<FormatCode>()));
    public static ResultColumnCodes CreateOverall(FormatCode code) => new(code);
    public static ResultColumnCodes CreatePerColumn(ArraySegment<FormatCode> codes) => new(codes);
}

readonly struct Bind: IStreamingFrontendMessage
{
    readonly string _portalName;
    readonly ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> _parameters;
    readonly FormatCode? _parametersOverallCode;
    readonly ResultColumnCodes _resultColumnCodes;
    readonly string _preparedStatementName;
    readonly int _precomputedMessageLength;

    public Bind(string portalName, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> parameters, ResultColumnCodes resultColumnCodes, string? preparedStatementName = null)
    {
        if (FrontendMessage.DebugEnabled && _parameters.Count > short.MaxValue)
            throw new InvalidOperationException($"Cannot accept more than short.MaxValue ({short.MaxValue} parameters.");

        if (FrontendMessage.DebugEnabled && _resultColumnCodes.IsPerColumnCodes && _resultColumnCodes.PerColumnCodes.Count > short.MaxValue)
            throw new InvalidOperationException($"Cannot accept more than short.MaxValue ({short.MaxValue} result columns.");

        var forall = true;
        FormatCode? formatCode = _parameters.Array is null ? null : _parameters.Array![0].Key.FormatCode;
        // Note offset + 1 to start at the second param.
        for (var i = _parameters.Offset + 1; i < _parameters.Count; i++)
        {
            if (formatCode != _parameters.Array![i].Key.FormatCode)
            {
                forall = false;
                break;
            }
        }

        if (forall)
            _parametersOverallCode = formatCode;

        _portalName = portalName;
        _parameters = parameters;
        _resultColumnCodes = resultColumnCodes;
        _preparedStatementName = preparedStatementName ?? string.Empty;
        _precomputedMessageLength = PrecomputeMessageLength();
    }

    public FrontendCode FrontendCode => FrontendCode.Bind;

    public bool TryPrecomputeLength(out int length)
    {
        // Whatever, something like segment size can come via the constructor too, if we want to get fancy.
        if (_precomputedMessageLength < 2048)
        {
            length = _precomputedMessageLength;
            return true;
        }

        length = default;
        return false;
    }

    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        buffer.WriteCString(_portalName);
        buffer.WriteCString(_preparedStatementName);

        WriteParameterCodes(ref buffer);

        var parameters = _parameters;
        buffer.WriteShort((short)parameters.Count);
        var lastBuffered = buffer.BufferedBytes;
        var lastCommitted = buffer.BytesCommitted + lastBuffered;
        for (var i = parameters.Offset; i < parameters.Count; i++)
        {
            var p = parameters.Array![i];
            p.Value.Write(ref buffer, p.Key.FormatCode, p.Key.Value);
            if (FrontendMessage.DebugEnabled)
                CheckParameterWriterOutput(p.Key.Length, lastBuffered, lastCommitted, buffer);

            lastCommitted += buffer.BufferedBytes - lastBuffered;
            lastBuffered = buffer.BufferedBytes;
        }

        WriteResultColumnCodes(ref buffer);
    }

    public async ValueTask<FlushResult> WriteWithHeaderAsync<T>(MessageWriter<T> writer, CancellationToken cancellationToken = default) where T : IBufferWriter<byte>
    {
        writer.WriteByte((byte)FrontendCode);
        writer.WriteInt(_precomputedMessageLength + MessageWriter.IntByteCount);

        writer.WriteCString(_portalName);
        writer.WriteCString(_preparedStatementName);

        WriteParameterCodes(ref writer.Writer);

        var parameters = _parameters;
        writer.WriteShort((short)parameters.Count);
        var lastBuffered = writer.BufferedBytes;
        var lastCommitted = writer.BytesCommitted + lastBuffered;
        for (var i = parameters.Offset; i < parameters.Count; i++)
        {
            var p = parameters.Array![i];
            p.Value.Write(ref writer.Writer, p.Key.FormatCode, p.Key.Value);
            if (FrontendMessage.DebugEnabled)
                CheckParameterWriterOutput(p.Key.Length, lastBuffered, lastCommitted, writer.Writer);

            // Make sure we don't commit too often, as this requires a memory slice in the pipe
            // additionally any writer loop may start writing small packets if we let it know certain memory is returned.
            if (writer.BufferedBytes > writer.AdvisoryFlushThreshold)
            {
                writer.Writer.Commit();
                var result = await writer.FlushAsync(cancellationToken);
                if (result.IsCanceled || result.IsCompleted) return result;
                lastCommitted = writer.BytesCommitted;
                lastBuffered = 0;
            }

            lastCommitted += writer.BufferedBytes - lastBuffered;
            lastBuffered = writer.BufferedBytes;
        }

        WriteResultColumnCodes(ref writer.Writer);
        return new FlushResult(isCanceled: false, isCompleted: false, isIgnored: false);
    }

    int PrecomputeMessageLength()
    {
        var parameters = _parameters;
        var length =
            MessageWriter.GetCStringByteCount(_portalName) +
            MessageWriter.GetCStringByteCount(_preparedStatementName) +
            MessageWriter.ShortByteCount + // Number of parameter codes
            (_parametersOverallCode is not null ? MessageWriter.ShortByteCount : parameters.Count * MessageWriter.ShortByteCount) +
            MessageWriter.ShortByteCount + // Number of parameter values
            (_resultColumnCodes.IsOverallCode
                ? MessageWriter.ShortByteCount * 2
                : MessageWriter.ShortByteCount + _resultColumnCodes.PerColumnCodes.Count * MessageWriter.ShortByteCount);

        for (var i = parameters.Offset; i < parameters.Count && i < parameters.Array!.Length; i++)
        {
            length += parameters.Array[i].Key.Length;
        }

        return length;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void CheckParameterWriterOutput<T>(int parameterLength, long lastBuffered, long lastCommitted, BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        if (buffer.BufferedBytes - lastBuffered < 4)
            throw new InvalidOperationException("A parameter writer should at least write 4 bytes for the length.");
        if (buffer.BytesCommitted > lastCommitted)
            throw new InvalidOperationException("Parameter writers should not call writer.Commit(), this is handled globally.");
        if (buffer.BytesCommitted + buffer.BufferedBytes - lastCommitted > parameterLength)
            throw new InvalidOperationException("The parameter writer output was not consistent with the parameter length.");
    }

    void WriteParameterCodes<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        if (_parameters.Count == 0)
        {
            buffer.WriteShort(0);
            return;
        }

        if (_parametersOverallCode is not null)
        {
            buffer.WriteShort(1);
            buffer.WriteShort((short)_parametersOverallCode);
        }
        else
        {
            buffer.WriteShort((short)_parameters.Count);
            for (var i = _parameters.Offset; i < _parameters.Count; i++)
            {
                buffer.WriteShort((short)_parameters.Array![i].Key.FormatCode);
            }
        }
    }

    void WriteResultColumnCodes<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        if (_resultColumnCodes.IsOverallCode)
        {
            buffer.WriteShort(1);
            buffer.WriteShort((short)_resultColumnCodes.OverallCode);
        }
        else
        {
            buffer.WriteShort((short)_resultColumnCodes.PerColumnCodes.Count);
            for (var i = _resultColumnCodes.PerColumnCodes.Offset; i < _resultColumnCodes.PerColumnCodes.Count; i++)
            {
                buffer.WriteShort((short)_resultColumnCodes.PerColumnCodes.Array![i]);
            }
        }
    }
}

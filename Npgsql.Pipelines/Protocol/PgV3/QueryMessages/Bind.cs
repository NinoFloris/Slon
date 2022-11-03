using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;
using Npgsql.Pipelines.Protocol.PgV3.Types;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct ResultColumnCodes
{
    ResultColumnCodes(FormatCode code) => OverallCode = code;
    ResultColumnCodes(ReadOnlyMemory<FormatCode> codes) => PerColumnCodes = codes;

    public bool IsOverallCode => PerColumnCodes.IsEmpty;
    public bool IsPerColumnCodes => !IsOverallCode;

    public FormatCode OverallCode { get; }
    public ReadOnlyMemory<FormatCode> PerColumnCodes { get; }

    public static ResultColumnCodes NoColumns => new(ReadOnlyMemory<FormatCode>.Empty);
    public static ResultColumnCodes CreateOverall(FormatCode code) => new(code);
    public static ResultColumnCodes CreatePerColumn(ReadOnlyMemory<FormatCode> codes) => new(codes);
}

readonly struct Bind: IFrontendMessage
{
    readonly string _portalName;
    readonly ReadOnlyMemory<KeyValuePair<CommandParameter, IParameterWriter>> _parameters;
    readonly FormatCode? _parametersOverallCode;
    readonly ResultColumnCodes _resultColumnCodes;
    readonly string _preparedStatementName;
    readonly int _precomputedMessageLength;

    public Bind(string portalName, ReadOnlyMemory<KeyValuePair<CommandParameter, IParameterWriter>> parameters, ResultColumnCodes resultColumnCodes, string? preparedStatementName)
    {
        if (FrontendMessage.DebugEnabled && _parameters.Length > Parameter.Maximum)
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({Parameter.Maximum} parameters.");

        if (FrontendMessage.DebugEnabled && _resultColumnCodes.IsPerColumnCodes && _resultColumnCodes.PerColumnCodes.Length > Parameter.Maximum)
            throw new InvalidOperationException($"Cannot accept more than short.MaxValue ({Parameter.Maximum} result columns.");

        var forall = true;
        FormatCode? formatCode = _parameters.IsEmpty ? null : ((PgV3ProtocolParameterType)_parameters.Span[0].Key.Type).FormatCode;
        // Note i = 1 to start at the second param.
        for (var i = 1; i < _parameters.Length; i++)
        {
            if (formatCode != ((PgV3ProtocolParameterType)_parameters.Span[0].Key.Type).FormatCode)
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

    // Whatever, something like segment size can come via the constructor too, if we want to get fancy.
    public bool CanWrite => _precomputedMessageLength < 2048;

    public void Write<T>(ref SpanBufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.Create(FrontendCode.Bind, _precomputedMessageLength).Write(ref buffer);
        buffer.WriteCString(_portalName);
        buffer.WriteCString(_preparedStatementName);

        WriteParameterCodes(ref buffer);

        var parameters = _parameters;
        buffer.WriteUShort((ushort)parameters.Length);
        if (!parameters.IsEmpty)
        {
            var lastBuffered = buffer.BufferedBytes;
            var lastCommitted = buffer.BytesCommitted + lastBuffered;
            foreach (var (key, value) in _parameters.Span)
            {
                value.Write(ref buffer, key);
                if (FrontendMessage.DebugEnabled && key.PrecomputedLength.HasValue)
                    CheckParameterWriterOutput(key.PrecomputedLength.Value, lastBuffered, lastCommitted, buffer);

                lastCommitted += buffer.BufferedBytes - lastBuffered;
                lastBuffered = buffer.BufferedBytes;
            }
        }

        WriteResultColumnCodes(ref buffer);
    }

    public async ValueTask<FlushResult> WriteAsync<T>(MessageWriter<T> writer, CancellationToken cancellationToken = default) where T : IBufferWriter<byte>
    {
        writer.WriteByte((byte)FrontendCode.Bind);
        writer.WriteInt(_precomputedMessageLength + MessageWriter.IntByteCount);

        writer.WriteCString(_portalName);
        writer.WriteCString(_preparedStatementName);

        WriteParameterCodes(ref writer.Writer);

        var parameters = _parameters;
        writer.WriteUShort((ushort)parameters.Length);
        if (!parameters.IsEmpty)
        {
            var lastBuffered = writer.BufferedBytes;
            var lastCommitted = writer.BytesCommitted + lastBuffered;
            for (var i = 0; i < _parameters.Span.Length; i++)
            {
                var (key, value) = _parameters.Span[i];
                value.Write(ref writer.Writer, key);
                if (FrontendMessage.DebugEnabled && key.PrecomputedLength.HasValue)
                    CheckParameterWriterOutput(key.PrecomputedLength.Value, lastBuffered, lastCommitted, writer.Writer);

                // Make sure we don't commit too often, as this requires a memory slice in the pipe
                // additionally any writer loop may start writing small packets if we let it know certain memory is returned.
                if (writer.BufferedBytes > writer.AdvisoryFlushThreshold)
                {
                    var result = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                    if (result.IsCanceled || result.IsCompleted) return result;
                    lastCommitted = writer.BytesCommitted;
                    lastBuffered = 0;
                }

                lastCommitted += writer.BufferedBytes - lastBuffered;
                lastBuffered = writer.BufferedBytes;
            }
        }

        WriteResultColumnCodes(ref writer.Writer);
        return new FlushResult(isCanceled: false, isCompleted: false);
    }

    int PrecomputeMessageLength()
    {
        var parameters = _parameters;
        var length =
            MessageWriter.GetCStringByteCount(_portalName) +
            MessageWriter.GetCStringByteCount(_preparedStatementName) +
            MessageWriter.ShortByteCount + // Number of parameter codes
            (_parametersOverallCode is not null ? MessageWriter.ShortByteCount : parameters.Length * MessageWriter.ShortByteCount) +
            MessageWriter.ShortByteCount + // Number of parameter values
            (_resultColumnCodes.IsOverallCode
                ? MessageWriter.ShortByteCount * 2
                : MessageWriter.ShortByteCount + _resultColumnCodes.PerColumnCodes.Length * MessageWriter.ShortByteCount);

        foreach (var (key, _) in _parameters.Span)
        {
            if (!key.PrecomputedLength.HasValue)
                throw new InvalidOperationException("Every postgres parameter requires a precomputed length.");
            length += key.PrecomputedLength.Value;
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

    [MethodImpl(MethodImplOptions.NoInlining)]
    void CheckParameterWriterOutput<T>(int parameterLength, long lastBuffered, long lastCommitted, SpanBufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        if (buffer.BufferedBytes - lastBuffered < 4)
            throw new InvalidOperationException("A parameter writer should at least write 4 bytes for the length.");
        if (buffer.BytesCommitted > lastCommitted)
            throw new InvalidOperationException("Parameter writers should not call writer.Commit(), this is handled globally.");
        if (buffer.BytesCommitted + buffer.BufferedBytes - lastCommitted > parameterLength)
            throw new InvalidOperationException("The parameter writer output was not consistent with the parameter length.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void WriteParameterCodes<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        if (_parameters.Length == 0)
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
            buffer.WriteUShort((ushort)_parameters.Length);
            foreach (var (key, _) in _parameters.Span)
                buffer.WriteShort((short)((PgV3ProtocolParameterType)_parameters.Span[0].Key.Type).FormatCode);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void WriteParameterCodes<T>(ref SpanBufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        if (_parameters.Length == 0)
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
            buffer.WriteUShort((ushort)_parameters.Length);
            foreach (var (key, _) in _parameters.Span)
                buffer.WriteShort((short)((PgV3ProtocolParameterType)_parameters.Span[0].Key.Type).FormatCode);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void WriteResultColumnCodes<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        if (_resultColumnCodes.IsOverallCode)
        {
            buffer.WriteShort(1);
            buffer.WriteShort((short)_resultColumnCodes.OverallCode);
        }
        else
        {
            buffer.WriteShort((short)_resultColumnCodes.PerColumnCodes.Length);
            foreach (var code in _resultColumnCodes.PerColumnCodes.Span)
                buffer.WriteShort((short)code);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void WriteResultColumnCodes<T>(ref SpanBufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        if (_resultColumnCodes.IsOverallCode)
        {
            buffer.WriteShort(1);
            buffer.WriteShort((short)_resultColumnCodes.OverallCode);
        }
        else
        {
            buffer.WriteShort((short)_resultColumnCodes.PerColumnCodes.Length);
            foreach (var code in _resultColumnCodes.PerColumnCodes.Span)
                buffer.WriteShort((short)code);
        }
    }
}

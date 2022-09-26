using System;
using System.Buffers;
using System.Collections.Generic;
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
        if (_parameters.Count > short.MaxValue)
            throw new InvalidOperationException($"Cannot accept more than short.MaxValue ({short.MaxValue} parameters.");

        if (_resultColumnCodes.IsPerColumnCodes && _resultColumnCodes.PerColumnCodes.Count > short.MaxValue)
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
    public void Write<T>(MessageWriter<T> writer) where T : IBufferWriter<byte> => throw new NotSupportedException();
    public bool TryPrecomputeLength(out int length) => throw new NotSupportedException();
    public async ValueTask<FlushResult> WriteWithHeaderAsync<T>(MessageWriter<T> writer, CancellationToken cancellationToken = default) where T : IBufferWriter<byte>
    {
        writer.WriteByte((byte)FrontendCode);
        writer.WriteInt(_precomputedMessageLength);

        writer.WriteCString(_portalName);
        writer.WriteCString(_preparedStatementName);

        WriteParameterCodes(ref writer);

        writer.WriteShort((short)_parameters.Count);
        var lastBuffered = writer.BufferedBytes;
        var lastCommitted = writer.BytesCommitted + lastBuffered;
        for (var i = _parameters.Offset; i < _parameters.Count; i++)
        {
            var p = _parameters.Array![i];
            p.Value.Write(writer, p.Key.FormatCode, p.Key.Value);
            if (FrontendMessageDebug.Enabled)
            {
                if (writer.BufferedBytes - lastBuffered < 4)
                    throw new InvalidOperationException("Parameter writer should at least write 4 bytes for the length.");
                if (writer.BytesCommitted > lastCommitted)
                    throw new InvalidOperationException("Please don't call writer.Commit inside parameter writers, this is handled globally.");
                if (writer.BytesCommitted + writer.BufferedBytes - lastCommitted > p.Key.Length)
                    throw new InvalidOperationException("Parameter output too big, should have been caught in the parameter writer itself.");
            }

            // Make sure we don't commit too often, as this requires a memory slice in the pipe
            // additionally any writer loop may start writing small packets if we let it know certain memory is returned.
            if (writer.BufferedBytes > writer.CommitThreshold)
            {
                var result = await writer.FlushAsync(cancellationToken);
                if (result.IsCanceled || result.IsCompleted) return result;
                lastCommitted = writer.BytesCommitted;
                lastBuffered = 0;
            }

            lastCommitted += writer.BufferedBytes - lastBuffered;
            lastBuffered = writer.BufferedBytes;
        }

        WriteResultColumnCodes(ref writer);
        return await writer.FlushAsync(cancellationToken);
    }

    int PrecomputeMessageLength()
    {
        var length = MessageWriter.IntByteCount;
        length += MessageWriter.GetCStringByteCount(_portalName);
        length += MessageWriter.GetCStringByteCount(_preparedStatementName);
        if (_parameters.Count == 0)
            length += MessageWriter.ShortByteCount;
        else if (_parametersOverallCode is not null)
            length += MessageWriter.ShortByteCount * 2;
        else
            length += MessageWriter.ShortByteCount + _parameters.Count * MessageWriter.ShortByteCount;

        length += MessageWriter.ShortByteCount;

        for (int i = _parameters.Offset; i < _parameters.Count; i++)
        {
            length += _parameters.Array![i].Key.Length;
        }

        if (_resultColumnCodes.IsOverallCode)
            length += MessageWriter.ShortByteCount * 2;
        else
            length += MessageWriter.ShortByteCount + _resultColumnCodes.PerColumnCodes.Count * MessageWriter.ShortByteCount;

        return length;
    }

    void WriteParameterCodes<T>(ref MessageWriter<T> writer) where T : IBufferWriter<byte>
    {
        if (_parameters.Count == 0)
        {
            writer.WriteShort(0);
            return;
        }

        if (_parametersOverallCode is not null)
        {
            writer.WriteShort(1);
            writer.WriteShort((short)_parametersOverallCode);
        }
        else
        {
            writer.WriteShort((short)_parameters.Count);
            for (var i = _parameters.Offset; i < _parameters.Count; i++)
            {
                writer.WriteShort((short)_parameters.Array![i].Key.FormatCode);
            }
        }
    }

    void WriteResultColumnCodes<T>(ref MessageWriter<T> writer) where T : IBufferWriter<byte>
    {
        if (_resultColumnCodes.IsOverallCode)
        {
            writer.WriteShort(1);
            writer.WriteShort((short)_resultColumnCodes.OverallCode);
        }
        else
        {
            writer.WriteShort((short)_resultColumnCodes.PerColumnCodes.Count);
            for (var i = _resultColumnCodes.PerColumnCodes.Offset; i < _resultColumnCodes.PerColumnCodes.Count; i++)
            {
                writer.WriteShort((short)_resultColumnCodes.PerColumnCodes.Array![i]);
            }
        }
    }
}

using System;
using System.Buffers;
using System.Collections.Generic;

namespace Npgsql.Pipelines.QueryMessages;

readonly struct Parse: IFrontendMessage
{
    readonly string _commandText;
    readonly ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> _parameters;
    readonly string _preparedStatementName;

    public Parse(string commandText, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> parameters, string? preparedStatementName = null)
    {
        if (_parameters.Count > short.MaxValue)
            throw new InvalidOperationException($"Cannot accept more than short.MaxValue ({short.MaxValue} parameters.");

        _commandText = commandText;
        _parameters = parameters;
        _preparedStatementName = preparedStatementName ?? string.Empty;
    }

    public FrontendCode FrontendCode => FrontendCode.Parse;

    public void Write<T>(MessageWriter<T> writer) where T : IBufferWriter<byte>
    {
        writer.WriteCString(_preparedStatementName);
        writer.WriteCString(_commandText);
        writer.WriteShort((short)_parameters.Count);

        for (var i = _parameters.Offset; i < _parameters.Count; i++)
        {
            var p = _parameters.Array![i];
            writer.WriteInt(p.Key.Oid.Value);
        }
    }

    public bool TryPrecomputeLength(out int length)
    {
        length = MessageWriter.GetCStringByteCount(_preparedStatementName) +
                 MessageWriter.GetCStringByteCount(_commandText) +
                 MessageWriter.ShortByteCount +
                 (MessageWriter.IntByteCount * _parameters.Count);
        return true;
    }
}

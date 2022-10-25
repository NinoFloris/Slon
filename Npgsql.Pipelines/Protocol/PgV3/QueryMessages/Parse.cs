using System;
using System.Buffers;
using System.Collections.Generic;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Parse: IFrontendMessage
{
    readonly string _commandText;
    readonly ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> _parameters;
    readonly string _preparedStatementName;
    readonly int _precomputedLength;

    public Parse(string commandText, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> parameters, string? preparedStatementName)
    {
        if (FrontendMessage.DebugEnabled && _parameters.Count > ushort.MaxValue)
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({ushort.MaxValue} parameters.");

        _commandText = commandText;
        _parameters = parameters;
        _preparedStatementName = preparedStatementName ?? string.Empty;
        _precomputedLength =
            MessageWriter.GetCStringByteCount(_preparedStatementName) +
            MessageWriter.GetCStringByteCount(_commandText) +
            MessageWriter.ShortByteCount +
            (MessageWriter.IntByteCount * _parameters.Count);
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.Create(FrontendCode.Parse, _precomputedLength).Write(ref buffer);
        buffer.WriteCString(_preparedStatementName);
        buffer.WriteCString(_commandText);
        buffer.WriteUShort((ushort)_parameters.Count);

        for (var i = _parameters.Offset; i < _parameters.Count; i++)
        {
            var p = _parameters.Array![i];
            buffer.WriteInt(p.Key.Oid.Value);
        }
    }
}

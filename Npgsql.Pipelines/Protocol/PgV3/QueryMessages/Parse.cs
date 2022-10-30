using System;
using System.Buffers;
using System.Collections.Generic;
using Npgsql.Pipelines.Protocol.PgV3.Types;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Parse: IFrontendMessage
{
    readonly string _commandText;
    readonly ReadOnlyMemory<KeyValuePair<CommandParameter, IParameterWriter>> _parameters;
    readonly string _preparedStatementName;
    readonly int _precomputedLength;

    public Parse(string commandText, ReadOnlyMemory<KeyValuePair<CommandParameter, IParameterWriter>> parameters, string? preparedStatementName)
    {
        if (FrontendMessage.DebugEnabled && _parameters.Length > Parameter.Maximum)
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({Parameter.Maximum} parameters.");

        _commandText = commandText;
        _parameters = parameters;
        _preparedStatementName = preparedStatementName ?? string.Empty;
        _precomputedLength =
            MessageWriter.GetCStringByteCount(_preparedStatementName) +
            MessageWriter.GetCStringByteCount(_commandText) +
            MessageWriter.ShortByteCount +
            (MessageWriter.IntByteCount * _parameters.Length);
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.Create(FrontendCode.Parse, _precomputedLength).Write(ref buffer);
        buffer.WriteCString(_preparedStatementName);
        buffer.WriteCString(_commandText);
        buffer.WriteUShort((ushort)_parameters.Length);

        foreach (var (key, _) in _parameters.Span)
            buffer.WriteInt(((PgV3ProtocolParameterType)key.Type).Parameter.Oid);
    }
}

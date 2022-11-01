using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
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
        _precomputedLength = PrecomputeLength(commandText, parameters, _preparedStatementName);
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
        => WriteMessage(ref buffer, _commandText, _parameters, _preparedStatementName, _precomputedLength);

    public static void WriteMessage<T>(ref BufferWriter<T> buffer, string commandText, in ReadOnlyMemory<KeyValuePair<CommandParameter, IParameterWriter>> parameters, string? preparedStatementName, int precomputedLength = -1)
        where T : IBufferWriter<byte>
    {
        if (FrontendMessage.DebugEnabled && parameters.Length > Parameter.Maximum)
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({Parameter.Maximum} parameters.");

        preparedStatementName ??= string.Empty;
        if (precomputedLength == -1)
            precomputedLength = PrecomputeLength(commandText, parameters, preparedStatementName);

        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Parse, precomputedLength);
        buffer.WriteCString(preparedStatementName);
        buffer.WriteCString(commandText);
        buffer.WriteUShort((ushort)parameters.Length);

        foreach (var (key, _) in parameters.Span)
            buffer.WriteInt(((PgV3ProtocolParameterType)key.Type).Parameter.Oid);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static int PrecomputeLength(string commandText, ReadOnlyMemory<KeyValuePair<CommandParameter, IParameterWriter>> parameters, string preparedStatementName) 
        => MessageWriter.GetCStringByteCount(preparedStatementName) +
           MessageWriter.GetCStringByteCount(commandText) +
           MessageWriter.ShortByteCount +
           (MessageWriter.IntByteCount * parameters.Length);
}

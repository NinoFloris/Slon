using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Npgsql.Pipelines.Protocol.PgV3.Descriptors;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Parse: IFrontendMessage
{
    readonly string _commandText;
    readonly ReadOnlyMemory<KeyValuePair<CommandParameter, ParameterWriter>> _parameters;
    readonly string _preparedStatementName;
    readonly int _precomputedLength;

    public Parse(string commandText, ReadOnlyMemory<KeyValuePair<CommandParameter, ParameterWriter>> parameters, string? preparedStatementName)
    {
        if (FrontendMessage.DebugEnabled && _parameters.Length > Parameter.MaxAmount)
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({Parameter.MaxAmount} parameters.");

        _commandText = commandText;
        _parameters = parameters;
        _preparedStatementName = preparedStatementName ?? string.Empty;
        _precomputedLength = PrecomputeLength(commandText, parameters, _preparedStatementName);
    }

    public bool CanWrite => true;
    public void Write<T>(ref SpanBufferWriter<T> buffer) where T : IBufferWriter<byte>
        => WriteMessage(ref buffer, _commandText, _parameters, _preparedStatementName, _precomputedLength);

    public static void WriteMessage<T>(scoped ref SpanBufferWriter<T> buffer, string commandText, scoped in ReadOnlyMemory<KeyValuePair<CommandParameter, ParameterWriter>> parameters, string? preparedStatementName, int precomputedLength = -1)
        where T : IBufferWriter<byte>
    {
        if (FrontendMessage.DebugEnabled && parameters.Length > Parameter.MaxAmount)
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({Parameter.MaxAmount} parameters.");

        preparedStatementName ??= string.Empty;
        if (precomputedLength == -1)
            precomputedLength = PrecomputeLength(commandText, parameters, preparedStatementName);

        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Parse, precomputedLength);
        buffer.WriteCString(preparedStatementName);
        buffer.WriteCString(commandText);
        buffer.WriteUShort((ushort)parameters.Length);

        foreach (var (key, _) in parameters.Span)
            buffer.WriteUInt(((PgV3ParameterDescriptor)key.Descriptor).Parameter.Oid);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static int PrecomputeLength(string commandText, ReadOnlyMemory<KeyValuePair<CommandParameter, ParameterWriter>> parameters, string preparedStatementName) 
        => MessageWriter.GetCStringByteCount(preparedStatementName) +
           MessageWriter.GetCStringByteCount(commandText) +
           MessageWriter.ShortByteCount +
           (MessageWriter.IntByteCount * parameters.Length);
}

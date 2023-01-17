using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Npgsql.Pipelines.Protocol.PgV3.Descriptors;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Parse: IFrontendMessage
{
    readonly string _commandText;
    readonly ReadOnlyMemory<KeyValuePair<CommandParameter, ParameterWriter>> _parameters;
    readonly Encoding _encoding;
    readonly string _preparedStatementName;
    readonly int _precomputedLength;

    public Parse(string commandText, ReadOnlyMemory<KeyValuePair<CommandParameter, ParameterWriter>> parameters, string? preparedStatementName, Encoding encoding)
    {
        if (FrontendMessage.DebugEnabled && _parameters.Length > Parameter.MaxAmount)
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({Parameter.MaxAmount} parameters.");

        _commandText = commandText;
        _parameters = parameters;
        _encoding = encoding;
        _preparedStatementName = preparedStatementName ?? string.Empty;
        _precomputedLength = PrecomputeLength(commandText, parameters, _preparedStatementName, encoding);
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
        => WriteMessage(ref buffer, _commandText, _parameters, _preparedStatementName, _encoding, _precomputedLength);

    public static void WriteMessage<T>(ref BufferWriter<T> buffer, string commandText, in ReadOnlyMemory<KeyValuePair<CommandParameter, ParameterWriter>> parameters, string? preparedStatementName, Encoding encoding, int precomputedLength = -1)
        where T : IBufferWriter<byte>
    {
        if (FrontendMessage.DebugEnabled && parameters.Length > Parameter.MaxAmount)
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({Parameter.MaxAmount} parameters.");

        preparedStatementName ??= string.Empty;
        if (precomputedLength == -1)
            precomputedLength = PrecomputeLength(commandText, parameters, preparedStatementName, encoding);

        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Parse, precomputedLength);
        buffer.WriteCString(preparedStatementName, encoding);
        buffer.WriteCString(commandText, encoding);
        buffer.WriteUShort((ushort)parameters.Length);

        foreach (var (key, _) in parameters.Span)
            buffer.WriteUInt((uint)((PgV3ParameterInfo)key.Info).Parameter.Oid);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static int PrecomputeLength(string commandText, ReadOnlyMemory<KeyValuePair<CommandParameter, ParameterWriter>> parameters, string preparedStatementName, Encoding encoding)
        => MessageWriter.GetCStringByteCount(preparedStatementName, encoding) +
           MessageWriter.GetCStringByteCount(commandText, encoding) +
           MessageWriter.ShortByteCount +
           (MessageWriter.IntByteCount * parameters.Length);
}

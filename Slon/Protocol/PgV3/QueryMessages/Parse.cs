using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using Slon.Buffers;
using Slon.Pg;
using Slon.Protocol.Pg;

namespace Slon.Protocol.PgV3;

readonly struct Parse: IFrontendMessage
{
    readonly PgTypeCatalog _pgTypeCatalog;
    readonly SizedString _statementText;
    readonly PooledMemory<Parameter> _parameters;
    readonly Encoding _encoding;
    readonly SizedString _preparedStatementName;
    readonly int _precomputedLength;

    public Parse(PgTypeCatalog pgTypeCatalog, SizedString statementText, PooledMemory<Parameter> parameters, SizedString? preparedStatementName, Encoding encoding)
    {
        // Bind always validates this invariant.
        if (FrontendMessage.DebugEnabled && _parameters.Length > Descriptors.StatementParameter.MaxCount)
            // Throw inlined as constructors will never be inlined, additionally DebugEnabled is not expected to be on in perf critical envs.
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({Descriptors.StatementParameter.MaxCount} parameters.");

        _pgTypeCatalog = pgTypeCatalog;
        _statementText = statementText.EnsureByteCount(encoding);
        _parameters = parameters;
        _preparedStatementName = preparedStatementName?.EnsureByteCount(encoding) ?? SizedString.Empty;
        _encoding = encoding;
        _precomputedLength = ComputeLength(_statementText, parameters, _preparedStatementName, encoding);
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
        => WriteMessageCore(ref buffer, _pgTypeCatalog, _statementText, _parameters, _preparedStatementName, _encoding, _precomputedLength);

    public static void WriteMessage<T>(ref BufferWriter<T> buffer, PgTypeCatalog pgTypeCatalog, SizedString statementText, PooledMemory<Parameter> parameters, SizedString? preparedStatementName, Encoding encoding)
        where T : IBufferWriter<byte>
        => WriteMessageCore(ref buffer, pgTypeCatalog, statementText, parameters, preparedStatementName, encoding);

    static void WriteMessageCore<T>(ref BufferWriter<T> buffer, PgTypeCatalog pgTypeCatalog, SizedString statementText, PooledMemory<Parameter> parameters, SizedString? preparedStatementName, Encoding encoding, int precomputedLength = -1)
        where T : IBufferWriter<byte>
    {
        // Bind always validates this invariant.
        if (FrontendMessage.DebugEnabled && parameters.Length > Descriptors.StatementParameter.MaxCount)
            // Throw inlined as constructors will never be inlined, additionally DebugEnabled is not expected to be on in perf critical envs.
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({Descriptors.StatementParameter.MaxCount} parameters.");

        statementText = statementText.EnsureByteCount(encoding);
        var statementName = preparedStatementName?.EnsureByteCount(encoding) ?? SizedString.Empty;
        if (precomputedLength == -1)
            precomputedLength = ComputeLength(statementText, parameters, statementName, encoding);

        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Parse, precomputedLength);
        buffer.WriteCString(statementName, encoding);
        buffer.WriteCString(statementText, encoding);
        buffer.WriteUShort((ushort)parameters.Length);

        if (!parameters.IsEmpty)
        {
            // This is slightly warty but it's really only overhead in portable data sources cases.
            foreach (var parameter in parameters.Span)
                buffer.WriteUInt((uint)parameter.GetOid(pgTypeCatalog));
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static int ComputeLength(SizedString statementText, PooledMemory<Parameter> parameters, SizedString preparedStatementName, Encoding encoding)
        => MessageWriter.GetCStringByteCount(preparedStatementName, encoding) +
           MessageWriter.GetCStringByteCount(statementText, encoding) +
           MessageWriter.ShortByteCount +
           (MessageWriter.IntByteCount * parameters.Length);
}

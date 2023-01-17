using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using Npgsql.Pipelines.Pg;
using Npgsql.Pipelines.Protocol.Pg;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Parse: IFrontendMessage
{
    readonly PgTypeCatalog _pgTypeCatalog;
    readonly SizedString _statementText;
    readonly PooledMemory<Parameter> _parameters;
    readonly Encoding _encoding;
    readonly PgV3Statement? _statement;
    readonly SizedString _preparedStatementName;
    readonly int _precomputedLength;

    public Parse(PgTypeCatalog pgTypeCatalog, SizedString statementText, PooledMemory<Parameter> parameters, SizedString? preparedStatementName, Encoding encoding, PgV3Statement? statement = null)
    {
        // Bind always validates this invariant.
        if (FrontendMessage.DebugEnabled && _parameters.Length > Descriptors.StatementParameter.MaxCount)
            // Throw inlined as constructors will never be inlined, additionally DebugEnabled is not expected to be on in perf critical envs.
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({Descriptors.StatementParameter.MaxCount} parameters.");

        _pgTypeCatalog = pgTypeCatalog;
        _statementText = statementText.EnsureByteCount(encoding);
        _parameters = parameters;
        _preparedStatementName = preparedStatementName?.EnsureByteCount(encoding) ?? string.Empty;
        _encoding = encoding;
        _statement = statement;
        _precomputedLength = ComputeLength(_statementText, parameters, _preparedStatementName, encoding);
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
        => WriteMessageCore(ref buffer, _pgTypeCatalog, _statementText, _parameters, _preparedStatementName, _encoding, _statement, _precomputedLength);

    public static void WriteMessage<T>(ref BufferWriter<T> buffer, PgTypeCatalog pgTypeCatalog, SizedString statementText, PooledMemory<Parameter> parameters, SizedString? preparedStatementName, Encoding encoding, PgV3Statement? statement = null)
        where T : IBufferWriter<byte>
        => WriteMessageCore(ref buffer, pgTypeCatalog, statementText, parameters, preparedStatementName, encoding, statement);

    static void WriteMessageCore<T>(ref BufferWriter<T> buffer, PgTypeCatalog pgTypeCatalog, SizedString statementText, PooledMemory<Parameter> parameters, SizedString? preparedStatementName, Encoding encoding, PgV3Statement? statement = null, int precomputedLength = -1)
        where T : IBufferWriter<byte>
    {
        // Bind always validates this invariant.
        if (FrontendMessage.DebugEnabled && parameters.Length > Descriptors.StatementParameter.MaxCount)
            // Throw inlined as constructors will never be inlined, additionally DebugEnabled is not expected to be on in perf critical envs.
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({Descriptors.StatementParameter.MaxCount} parameters.");

        statementText = statementText.EnsureByteCount(encoding);
        var statementName = preparedStatementName?.EnsureByteCount(encoding) ?? string.Empty;
        if (precomputedLength == -1)
            precomputedLength = ComputeLength(statementText, parameters, statementName, encoding);

        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Parse, precomputedLength);
        buffer.WriteCString(statementName, encoding);
        buffer.WriteCString(statementText, encoding);
        buffer.WriteUShort((ushort)parameters.Length);

        if (!parameters.IsEmpty)
        {
            // When we're preparing we have already had to compute the oids.
            if (statement is not null)
                foreach (var parameter in statement.Parameters)
                    buffer.WriteUInt((uint)parameter.Oid);
            // This is slightly warty but it's really only used in backend agnostic data sources cases.
            else
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

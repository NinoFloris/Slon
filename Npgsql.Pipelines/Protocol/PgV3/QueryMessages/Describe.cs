using System.Buffers;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Describe: IFrontendMessage
{
    readonly string _name;
    readonly bool _isPortalName;

    Describe(string name, bool isPortalName)
    {
        _name = name;
        _isPortalName = isPortalName;
    }

    public bool CanWrite => true;
    public void Write<T>(ref SpanBufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        if (_isPortalName)
            WriteForPortal(ref buffer, _name);
        else
            WriteForPreparedStatement(ref buffer, _name);
    }

    enum StatementOrPortal : byte
    {
        Statement = (byte)'S',
        Portal = (byte)'P'
    }

    public static void WriteForPreparedStatement<T>(ref SpanBufferWriter<T> buffer, string preparedStatementName) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Describe, MessageWriter.ByteByteCount + MessageWriter.GetCStringByteCount(preparedStatementName));
        buffer.WriteByte((byte)StatementOrPortal.Statement);
        buffer.WriteCString(preparedStatementName);
    }

    public static void WriteForPortal<T>(ref SpanBufferWriter<T> buffer, string portalName) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Describe, MessageWriter.ByteByteCount + MessageWriter.GetCStringByteCount(portalName));
        buffer.WriteByte((byte)StatementOrPortal.Portal);
        buffer.WriteCString(portalName);
    }

    public static Describe CreateForPreparedStatement(string preparedStatementName) => new(preparedStatementName, false);
    public static Describe CreateForPortal(string portalName) => new(portalName, true);
}

using System.Buffers;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Describe: IFrontendMessage
{
    string _name { get; }
    bool _isPortalName { get; }

    Describe(string name, bool isPortalName)
    {
        _name = name;
        _isPortalName = isPortalName;
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.Create(FrontendCode.Describe, MessageWriter.ByteByteCount + MessageWriter.GetCStringByteCount(_name)).Write(ref buffer);
        buffer.WriteByte((byte)(_isPortalName ? StatementOrPortal.Portal : StatementOrPortal.Statement));
        buffer.WriteCString(_name);
    }

    enum StatementOrPortal : byte
    {
        Statement = (byte)'S',
        Portal = (byte)'P'
    }

    public static Describe CreateForPreparedStatement(string preparedStatementName) => new(preparedStatementName, false);
    public static Describe CreateForPortal(string portalName) => new(portalName, true);
}

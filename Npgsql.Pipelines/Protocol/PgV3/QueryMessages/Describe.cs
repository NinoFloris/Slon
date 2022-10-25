using System.Buffers;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct DescribeName
{
    public string Name { get; }
    public bool IsPortalName { get; }

    DescribeName(string name, bool isPortalName)
    {
        Name = name;
        IsPortalName = isPortalName;
    }

    public static DescribeName CreateForPreparedStatement(string preparedStatementName) => new(preparedStatementName, false);
    public static DescribeName CreateForPortal(string portalName) => new(portalName, true);
}

readonly struct Describe: IFrontendMessage
{
    enum StatementOrPortal : byte
    {
        Statement = (byte) 'S',
        Portal = (byte) 'P'
    }

    readonly DescribeName _name;

    public Describe(DescribeName name) => _name = name;

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.Create(FrontendCode.Describe, MessageWriter.ByteByteCount + MessageWriter.GetCStringByteCount(_name.Name)).Write(ref buffer);
        buffer.WriteByte((byte)(_name.IsPortalName ? StatementOrPortal.Portal : StatementOrPortal.Statement));
        buffer.WriteCString(_name.Name);
    }
}

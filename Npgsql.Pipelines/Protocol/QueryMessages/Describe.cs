using System.Buffers;

namespace Npgsql.Pipelines.QueryMessages;

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

    public FrontendCode FrontendCode => FrontendCode.Describe;
    public void Write<T>(MessageWriter<T> writer) where T : IBufferWriter<byte>
    {
        writer.WriteByte((byte)(_name.IsPortalName ? StatementOrPortal.Portal : StatementOrPortal.Statement));
        writer.WriteCString(_name.Name);
        writer.Commit();
    }

    public bool TryPrecomputeLength(out int length)
    {
        length = MessageWriter.ByteByteCount + MessageWriter.GetCStringByteCount(_name.Name);
        return true;
    }
}

using System.Buffers;
using System.Text;

namespace Slon.Protocol.PgV3;

readonly struct Describe: IFrontendMessage
{
    readonly SizedString _name;
    readonly bool _isPortalName;
    readonly Encoding _encoding;

    Describe(SizedString name, bool isPortalName, Encoding encoding)
    {
        _name = name;
        _isPortalName = isPortalName;
        _encoding = encoding;
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        if (_isPortalName)
            WriteForPortal(ref buffer, _name, _encoding);
        else
            WriteForPreparedStatement(ref buffer, _name, _encoding);
    }

    enum StatementOrPortal : byte
    {
        Statement = (byte)'S',
        Portal = (byte)'P'
    }

    public static void WriteForPreparedStatement<T>(ref BufferWriter<T> buffer, SizedString preparedStatementName, Encoding encoding) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Describe, MessageWriter.ByteByteCount + MessageWriter.GetCStringByteCount(preparedStatementName, encoding));
        buffer.WriteByte((byte)StatementOrPortal.Statement);
        buffer.WriteCString(preparedStatementName, encoding);
    }

    public static void WriteForPortal<T>(ref BufferWriter<T> buffer, string portalName, Encoding encoding) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Describe, MessageWriter.ByteByteCount + MessageWriter.GetCStringByteCount(portalName, encoding));
        buffer.WriteByte((byte)StatementOrPortal.Portal);
        buffer.WriteCString(portalName, encoding);
    }

    public static Describe CreateForPreparedStatement(SizedString preparedStatementName, Encoding encoding) => new(preparedStatementName, false, encoding);
    public static Describe CreateForPortal(string portalName, Encoding encoding) => new((SizedString)portalName, true, encoding);
}

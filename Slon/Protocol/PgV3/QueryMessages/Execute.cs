using System.Buffers;
using System.Text;
using Slon.Buffers;

namespace Slon.Protocol.PgV3;

readonly struct Execute: IFrontendMessage
{
    readonly string _portalName;
    readonly Encoding _encoding;
    readonly int _rowCountLimit;

    public Execute(string portalName, Encoding encoding, int rowCountLimit = 0)
    {
        _portalName = portalName;
        _encoding = encoding;
        _rowCountLimit = rowCountLimit;
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
        => WriteMessage(ref buffer, _portalName, _encoding, _rowCountLimit);

    public static void WriteMessage<T>(ref BufferWriter<T> buffer, string portalName, Encoding encoding, int rowCountLimit = 0) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Execute, MessageWriter.GetCStringByteCount(portalName, encoding) + MessageWriter.IntByteCount);
        buffer.WriteCString(portalName, encoding);
        buffer.WriteInt(rowCountLimit);
    }
}

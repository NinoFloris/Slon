using System.Buffers;
using System.Text;
using Slon.Buffers;

namespace Slon.Protocol.PgV3;

readonly struct Close: IFrontendMessage
{
    enum CloseKind : byte
    {
        Statement = (byte) 'S',
        Portal = (byte) 'P',
    }

    readonly SizedString _preparedStatementName;
    readonly Encoding _encoding;

    public Close(SizedString preparedStatementName, Encoding encoding)
    {
        _preparedStatementName = preparedStatementName;
        _encoding = encoding;
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Close, MessageWriter.ByteByteCount + MessageWriter.GetCStringByteCount(_preparedStatementName, _encoding));
        buffer.WriteByte((byte)CloseKind.Statement);
        buffer.WriteCString(_preparedStatementName, _encoding);
    }
}

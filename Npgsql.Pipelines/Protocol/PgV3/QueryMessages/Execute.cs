using System.Buffers;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Execute: IFrontendMessage
{
    readonly string _portalName;
    readonly int _rowCountLimit;

    public Execute(string portalName, int rowCountLimit = 0)
    {
        _portalName = portalName;
        _rowCountLimit = rowCountLimit;
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.Create(FrontendCode.Execute, MessageWriter.GetCStringByteCount(_portalName) + MessageWriter.IntByteCount).Write(ref buffer);
        buffer.WriteCString(_portalName);
        buffer.WriteInt(_rowCountLimit);
    }
}

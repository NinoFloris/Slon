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
    public void Write<T>(ref SpanBufferWriter<T> buffer) where T : IBufferWriter<byte>
        => WriteMessage(ref buffer, _portalName, _rowCountLimit);

    public static void WriteMessage<T>(ref SpanBufferWriter<T> buffer, string portalName, int rowCountLimit = 0) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Execute, MessageWriter.GetCStringByteCount(portalName) + MessageWriter.IntByteCount);
        buffer.WriteCString(portalName);
        buffer.WriteInt(rowCountLimit);
    }
}

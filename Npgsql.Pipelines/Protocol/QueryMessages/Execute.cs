using System.Buffers;

namespace Npgsql.Pipelines.Protocol;

readonly struct Execute: IFrontendMessage
{
    readonly string _portalName;
    readonly int _rowCountLimit;

    public Execute(string portalName, int rowCountLimit = 0)
    {
        _portalName = portalName;
        _rowCountLimit = rowCountLimit;
    }

    public FrontendCode FrontendCode => FrontendCode.Execute;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        buffer.WriteCString(_portalName);
        buffer.WriteInt(_rowCountLimit);
    }

    public bool TryPrecomputeLength(out int length)
    {
        length = MessageWriter.GetCStringByteCount(_portalName) + MessageWriter.IntByteCount;
        return true;
    }
}

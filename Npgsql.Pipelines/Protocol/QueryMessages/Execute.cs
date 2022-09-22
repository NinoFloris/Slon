using System.Buffers;

namespace Npgsql.Pipelines.QueryMessages;

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
    public void Write<T>(MessageWriter<T> writer) where T : IBufferWriter<byte>
    {
        writer.WriteCString(_portalName);
        writer.WriteInt(_rowCountLimit);
        writer.Commit();
    }

    public bool TryPrecomputeLength(out int length)
    {
        length = MessageWriter.GetCStringByteCount(_portalName) + MessageWriter.IntByteCount;
        return true;
    }
}

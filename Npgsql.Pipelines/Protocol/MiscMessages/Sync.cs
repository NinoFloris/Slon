using System.Buffers;

namespace Npgsql.Pipelines.MiscMessages;

readonly struct Sync : IFrontendMessage
{
    public FrontendCode FrontendCode => FrontendCode.Sync;
    public void Write<T>(MessageWriter<T> writer) where T : IBufferWriter<byte>
    {
    }

    public bool TryPrecomputeLength(out int length)
    {
        length = 0;
        return true;
    }
}

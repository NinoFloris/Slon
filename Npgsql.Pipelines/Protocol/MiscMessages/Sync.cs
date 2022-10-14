using System.Buffers;

namespace Npgsql.Pipelines.Protocol;

readonly struct Sync : IFrontendMessage
{
    public FrontendCode FrontendCode => FrontendCode.Sync;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
    }

    public bool TryPrecomputeLength(out int length)
    {
        length = 0;
        return true;
    }
}

using System.Buffers;

namespace Npgsql.Pipelines.MiscMessages;

readonly struct Sync : IFrontendMessage
{
    public FrontendCode FrontendCode => FrontendCode.Sync;
    public void Write<T>(MessageWriter<T> writer) where T : IBufferWriter<byte>
    {
    }
}

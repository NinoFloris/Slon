using System.Buffers;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Sync : IFrontendMessage
{
    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.Create(FrontendCode.Sync, 0).Write(ref buffer);
    }
}

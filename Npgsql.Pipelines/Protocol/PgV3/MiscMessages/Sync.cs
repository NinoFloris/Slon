using System.Buffers;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Sync : IPgV3FrontendMessage
{
    public bool TryPrecomputeHeader(out PgV3FrontendHeader header)
    {
        header = PgV3FrontendHeader.Create(FrontendCode.Sync, 0);
        return true;
    }

    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
    }
}

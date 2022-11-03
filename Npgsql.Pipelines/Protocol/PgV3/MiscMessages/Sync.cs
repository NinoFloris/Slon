using System.Buffers;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Sync : IFrontendMessage
{
    public bool CanWrite => true;
    public void Write<T>(ref SpanBufferWriter<T> buffer) where T : IBufferWriter<byte>
        => WriteMessage(ref buffer);

    public static void WriteMessage<T>(ref SpanBufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Sync, 0);
    }
}

using System.Buffers;
using Slon.Buffers;

namespace Slon.Protocol.PgV3;

readonly struct Sync : IFrontendMessage
{
    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
        => WriteMessage(ref buffer);

    public static void WriteMessage<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Sync, 0);
    }
}

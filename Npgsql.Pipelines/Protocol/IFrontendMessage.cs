using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines;

public static class FrontendMessageDebug {
    public static bool Enabled { get; set; } = true;
}

enum FrontendCode: byte
{
    Describe = (byte) 'D',
    Sync = (byte) 'S',
    Execute = (byte) 'E',
    Parse = (byte) 'P',
    Bind = (byte) 'B',
    Close = (byte) 'C',
    Query = (byte) 'Q',
    CopyData = (byte) 'd',
    CopyDone = (byte) 'c',
    CopyFail = (byte) 'f',
    Terminate = (byte) 'X',
    Password = (byte) 'p',
}

interface IFrontendMessage
{
    FrontendCode FrontendCode { get; }
    void Write<T>(MessageWriter<T> writer) where T : IBufferWriter<byte>;
    bool TryPrecomputeLength(out int length);
}

interface IStreamingFrontendMessage: IFrontendMessage
{
    ValueTask<FlushResult> WriteWithHeaderAsync<T>(MessageWriter<T> writer, long flushThreshold = 8096, CancellationToken cancellationToken = default) where T : IFlushableBufferWriter<byte>;
}

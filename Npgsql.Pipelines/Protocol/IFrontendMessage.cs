using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines;

static class FrontendMessage {
    public static readonly bool DebugEnabled = false;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CannotPrecomputeLength(out int length)
    {
        length = default;
        return false;
    }
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
    bool TryPrecomputeLength(out int length);
    void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>;
}

interface IStreamingFrontendMessage: IFrontendMessage
{
    ValueTask<FlushResult> WriteWithHeaderAsync<T>(MessageWriter<T> writer, CancellationToken cancellationToken = default) where T : IBufferWriter<byte>;
}

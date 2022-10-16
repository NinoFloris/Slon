using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines.Protocol;

static class FrontendMessage {
    public static readonly bool DebugEnabled = false;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CannotPrecomputeHeader<THeader>(out THeader header) where THeader: struct
    {
        header = default;
        return false;
    }
}

interface IFrontendHeader<THeader> where THeader: struct, IFrontendHeader<THeader>
{
    public int HeaderLength { get; }
    public int Length { get; set;  }
    void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>;
}

interface IFrontendMessage<THeader> where THeader: struct, IFrontendHeader<THeader>
{
    bool TryPrecomputeHeader(out THeader header);
    void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>;
}

interface IStreamingFrontendMessage<THeader>: IFrontendMessage<THeader> where THeader : struct, IFrontendHeader<THeader>
{
    ValueTask<FlushResult> WriteWithHeaderAsync<T>(MessageWriter<T> writer, CancellationToken cancellationToken = default) where T : IBufferWriter<byte>;
}

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Internal;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines.StartupMessages;

class StartupRequest: IStreamingFrontendMessage
{
    readonly List<KeyValuePair<string, string>> _parameters;

    public StartupRequest(ConnectionOptions options)
    {
        _parameters = new(){
            new KeyValuePair<string, string>("user", options.Username),
            new KeyValuePair<string, string>("client_encoding", "UTF8")
        };
        if (options.Database is not null)
            _parameters.Add(new KeyValuePair<string, string>("database", options.Database));
    }

    public FrontendCode FrontendCode => throw new NotSupportedException();
    public void Write<T>(MessageWriter<T> writer) where T : IBufferWriter<byte> => throw new NotSupportedException();
    public bool TryPrecomputeLength(out int length) => throw new NotSupportedException();

    public ValueTask<FlushResult> WriteWithHeaderAsync<T>(MessageWriter<T> writer, long flushThreshold = 8096, CancellationToken cancellationToken = default) where T : IFlushableBufferWriter<byte>
    {
        // Getting the thread static is safe as long as we don't go async before returning it.
        var memWriter = new MessageWriter<MemoryBufferWriter>(MemoryBufferWriter.Get());
        try
        {
            const int protocolVersion3 = 3 << 16; // 196608
            memWriter.WriteInt(protocolVersion3);

            foreach (var kv in _parameters)
            {
                memWriter.WriteCString(kv.Key);
                memWriter.WriteCString(kv.Value);
            }

            memWriter.WriteByte(0);
            memWriter.Commit();

            writer.WriteInt(MessageWriter.IntByteCount + (int)memWriter.UnflushedBytes);
            writer.Commit();
            memWriter.Writer.CopyTo(writer.Writer);
            writer.AdvanceCommitted(memWriter.UnflushedBytes);
        }
        finally
        {
            MemoryBufferWriter.Return(memWriter.Writer);
        }

        return writer.FlushAsync(flushThreshold, cancellationToken);
    }
}

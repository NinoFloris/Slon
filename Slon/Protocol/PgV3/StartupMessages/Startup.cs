using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Internal;
using Slon.Buffers;
using Slon.Protocol.Pg;

namespace Slon.Protocol.PgV3;

readonly struct Startup: IStreamingFrontendMessage
{
    readonly List<KeyValuePair<string, string>> _parameters;
    readonly Encoding _encoding;

    public Startup(PgOptions options)
    {
        _parameters = new(){
            new KeyValuePair<string, string>("user", options.Username),
            new KeyValuePair<string, string>("client_encoding", options.Encoding.WebName)
        };
        if (options.Database is not null)
            _parameters.Add(new KeyValuePair<string, string>("database", options.Database));
        _encoding = options.Encoding;
    }

    public bool CanWrite => false;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte> => throw new NotSupportedException();

    public ValueTask<FlushResult> WriteAsync<T>(MessageWriter<T> writer, CancellationToken cancellationToken = default) where T : IStreamingWriter<byte>
    {
        // Getting the thread static is safe as long as we don't go async before returning it.
        var memWriter = new StreamingWriter<MemoryBufferWriter>(MemoryBufferWriter.Get());
        try
        {
            const int protocolVersion3 = 3 << 16; // 196608
            memWriter.WriteInt(protocolVersion3);

            foreach (var kv in _parameters)
            {
                memWriter.WriteCString(kv.Key, _encoding);
                memWriter.WriteCString(kv.Value, _encoding);
            }

            memWriter.WriteByte(0);
            memWriter.Commit();

            writer.WriteInt(MessageWriter.IntByteCount + (int)memWriter.BytesCommitted);
            memWriter.CopyTo(ref writer.Writer);
        }
        finally
        {
            MemoryBufferWriter.Return(memWriter.Output);
        }

        return writer.FlushAsync(cancellationToken);
    }
}

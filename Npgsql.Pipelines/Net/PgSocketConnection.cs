using System;
using System.IO.Pipelines;
using System.Net;
using System.Threading.Tasks;
using Pipelines.Sockets.Unofficial;

namespace Npgsql.Pipelines;

class PgSocketConnection: IDisposable
{
    readonly SocketConnection _connection;
    public PipeReader Reader { get; }
    public PipeWriter Writer { get; }

    PgSocketConnection(SocketConnection connection)
    {
        _connection = connection;
        Reader = _connection.Input;
        Writer = _connection.Output;
    }

    public PipeShutdownKind ShutdownKind => _connection.ShutdownKind;

    const int MinimumWriteBufferSize = 8096;
    const int MaxWriteBufferingOnPipe = 1024 * 1024;
    const int ResumeWriteBufferingOnPipe = MaxWriteBufferingOnPipe / 2;
    static PipeScheduler IOScheduler { get; } = PipeScheduler.Inline;
    static PipeOptions DefaultSendPipeOptions { get; } =
        new(null, IOScheduler, PipeScheduler.Inline, MaxWriteBufferingOnPipe, ResumeWriteBufferingOnPipe, MinimumWriteBufferSize, false);
    static PipeOptions DefaultReceivePipeOptions { get; } =
        new(null, PipeScheduler.Inline, IOScheduler, useSynchronizationContext: false);

    public static async ValueTask<PgSocketConnection> ConnectAsync(EndPoint endPoint)
        => new(await SocketConnection.ConnectAsync(endPoint, DefaultSendPipeOptions, DefaultReceivePipeOptions).ConfigureAwait(false));

    public void Dispose()
    {
        _connection.Dispose();
    }
}

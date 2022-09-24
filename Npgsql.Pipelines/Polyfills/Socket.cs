using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Pipelines.Sockets.Unofficial;

namespace System.Net.Sockets;

#if NETSTANDARD2_0

static class SocketExtensions
{
    public static async ValueTask ConnectAsync(this Socket socket, EndPoint remoteEP, CancellationToken cancellationToken = default)
    {
        // TODO probably want to use our own SAEA here.
        using (var args = new SocketAwaitableEventArgs((SocketConnectionOptions.InlineConnect) == 0 ? PipeScheduler.ThreadPool : null))
        {
            args.RemoteEndPoint = remoteEP;
            if (!socket.ConnectAsync(args)) args.Complete();
            await args;
        }
    }
}

#endif

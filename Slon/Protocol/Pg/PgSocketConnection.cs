using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Pipelines.Sockets.Unofficial;
using Slon.Pipelines;

namespace Slon.Protocol.Pg;

abstract class PgSocketConnection
{
    public abstract bool CanBlock { get; }
    public abstract PipeReader Reader { get; }
    public abstract PipeWriter Writer { get; }

    protected const int DefaultReaderSegmentSize = 8192;
    protected const int DefaultWriterSegmentSize = DefaultReaderSegmentSize;

    protected static Socket CreateUnconnectedSocket(EndPoint endPoint)
    {
        var protocolType =
            endPoint.AddressFamily == AddressFamily.InterNetwork ||
            endPoint.AddressFamily == AddressFamily.InterNetworkV6
                ? ProtocolType.Tcp
                : ProtocolType.IP;
        return WithDefaultSocketOptions(new Socket(endPoint.AddressFamily, SocketType.Stream, protocolType));
    }

    static Socket WithDefaultSocketOptions(Socket socket)
    {
        if (socket.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
            socket.NoDelay = true;
        return socket;
    }
}

sealed class PgPipeConnection: PgSocketConnection, IDisposable
{
    readonly SocketConnection _connection;

    PgPipeConnection(SocketConnection connection)
    {
        _connection = connection;
        Reader = connection.Input;
        Writer = new PipeWriterUnflushedBytes(connection.Output);
    }

    public override PipeReader Reader { get; }
    public override PipeWriter Writer { get; }
    public override bool CanBlock => false;

    public PipeShutdownKind ShutdownKind => _connection.ShutdownKind;

    const int MaxWriteBufferingOnPipe = 1024 * 1024;
    const int ResumeWriteBufferingOnPipe = MaxWriteBufferingOnPipe / 2;
    static PipeScheduler IOScheduler { get; } = PipeScheduler.Inline;
    static PipeScheduler AppScheduler { get; } = PipeScheduler.Inline;
    static PipeOptions DefaultSendPipeOptions { get; } =
        new(null, IOScheduler, AppScheduler, MaxWriteBufferingOnPipe, ResumeWriteBufferingOnPipe, DefaultWriterSegmentSize, false);
    static PipeOptions DefaultReceivePipeOptions { get; } =
        new(null, PipeScheduler.Inline, PipeScheduler.ThreadPool, minimumSegmentSize: DefaultReaderSegmentSize, useSynchronizationContext: false);

    public static async ValueTask<PgPipeConnection> ConnectAsync(EndPoint endPoint, CancellationToken cancellationToken = default)
    {
        var socket = CreateUnconnectedSocket(endPoint);
        await socket.ConnectAsync(endPoint, cancellationToken).ConfigureAwait(false);
        var sendOptions = new PipeOptions(DefaultSendPipeOptions.Pool, PipeScheduler.ThreadPool, new IOQueue(), DefaultSendPipeOptions.PauseWriterThreshold, DefaultSendPipeOptions.ResumeWriterThreshold, DefaultSendPipeOptions.MinimumSegmentSize);
        return new(SocketConnection.Create(socket, sendOptions, DefaultReceivePipeOptions));
    }

    public void Dispose()
    {
        _connection.Dispose();
    }
}

sealed class PgStreamConnection : PgSocketConnection, IDisposable, IAsyncDisposable
{
    readonly SealedNetworkStream _stream;

    PgStreamConnection(SealedNetworkStream stream)
    {
        _stream = stream;
        Reader = new StreamSyncCapablePipeReader(stream, new StreamPipeReaderOptions(bufferSize: DefaultReaderSegmentSize, useZeroByteReads: false));
        var writer = new StreamSyncCapablePipeWriter(stream, new StreamPipeWriterOptions(minimumBufferSize: DefaultWriterSegmentSize));
        Writer = writer;
    }

    public override PipeReader Reader { get; }
    public override PipeWriter Writer { get; }
    public override bool CanBlock => true;

    public static int WriterSegmentSize => DefaultWriterSegmentSize;

    public static async ValueTask<PgStreamConnection> ConnectAsync(EndPoint endPoint, CancellationToken cancellationToken = default)
    {
        var socket = CreateUnconnectedSocket(endPoint);
        await socket.ConnectAsync(endPoint, cancellationToken).ConfigureAwait(false);
        var stream = new SealedNetworkStream(socket, ownsSocket: true);
        return new PgStreamConnection(stream);
    }

    public static PgStreamConnection Connect(EndPoint endPoint, TimeSpan timeout = default)
    {
        var socket = CreateUnconnectedSocket(endPoint);
        ConnectWithTimeout();
        var stream = new SealedNetworkStream(socket, ownsSocket: true);
        return new PgStreamConnection(stream);

        void ConnectWithTimeout()
        {
            socket.Blocking = false;
            try
            {
                socket.Connect(endPoint);
            }
            catch (SocketException e)
            {
                if (e.SocketErrorCode != SocketError.WouldBlock)
                    throw;
            }
            var write = new List<Socket> {socket};
            var error = new List<Socket> {socket};
            Socket.Select(null, write, error, (int)timeout.Ticks / ((int)TimeSpan.TicksPerMillisecond / 10));
            var errorCode = (int) socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Error)!;
            if (errorCode != 0)
                throw new SocketException(errorCode);
            if (!write.Any())
                throw new TimeoutException("Timeout during connection attempt");
            socket.Blocking = true;
        }
    }

    public void Dispose()
    {
        Reader.Complete();
        Writer.Complete();
        _stream.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await Reader.CompleteAsync().ConfigureAwait(false);
        await Writer.CompleteAsync().ConfigureAwait(false);
#if !NETSTANDARD2_0
        await _stream.DisposeAsync().ConfigureAwait(false);
#else
        _stream.Dispose();
#endif
    }

    sealed class SealedNetworkStream : NetworkStream
    {
        public SealedNetworkStream(Socket socket) : base(socket)
        {
        }

        public SealedNetworkStream(Socket socket, bool ownsSocket) : base(socket, ownsSocket)
        {
        }

        public SealedNetworkStream(Socket socket, FileAccess access) : base(socket, access)
        {
        }

        public SealedNetworkStream(Socket socket, FileAccess access, bool ownsSocket) : base(socket, access, ownsSocket)
        {
        }
    }
}

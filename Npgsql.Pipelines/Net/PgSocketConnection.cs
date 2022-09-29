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

namespace Npgsql.Pipelines;

abstract class PgSocketConnection
{
    public abstract bool CanBlock { get; }
    public abstract IPipeReaderSyncSupport Reader { get; }
    public abstract IPipeWriterSyncSupport Writer { get; }

    protected const int DefaultWriterSegmentSize = 8192;
    protected const int DefaultReaderSegmentSize = 8192;

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
        if (socket.AddressFamily == AddressFamily.InterNetwork || socket.AddressFamily == AddressFamily.InterNetworkV6)
            socket.NoDelay = true;
        return socket;
    }
//         if (Settings.SocketReceiveBufferSize > 0)
//             socket.ReceiveBufferSize = Settings.SocketReceiveBufferSize;
//         if (Settings.SocketSendBufferSize > 0)
//             socket.SendBufferSize = Settings.SocketSendBufferSize;
//
//         if (Settings.TcpKeepAlive)
//             socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
//         if (Settings.TcpKeepAliveInterval > 0 && Settings.TcpKeepAliveTime == 0)
//             throw new ArgumentException("If TcpKeepAliveInterval is defined, TcpKeepAliveTime must be defined as well");
//         if (Settings.TcpKeepAliveTime > 0)
//         {
//             var timeSeconds = Settings.TcpKeepAliveTime;
//             var intervalSeconds = Settings.TcpKeepAliveInterval > 0
//                 ? Settings.TcpKeepAliveInterval
//                 : Settings.TcpKeepAliveTime;
//
// #if NETSTANDARD2_0 || NETSTANDARD2_1
//                 var timeMilliseconds = timeSeconds * 1000;
//                 var intervalMilliseconds = intervalSeconds * 1000;
//
//                 // For the following see https://msdn.microsoft.com/en-us/library/dd877220.aspx
//                 var uintSize = Marshal.SizeOf(typeof(uint));
//                 var inOptionValues = new byte[uintSize * 3];
//                 BitConverter.GetBytes((uint)1).CopyTo(inOptionValues, 0);
//                 BitConverter.GetBytes((uint)timeMilliseconds).CopyTo(inOptionValues, uintSize);
//                 BitConverter.GetBytes((uint)intervalMilliseconds).CopyTo(inOptionValues, uintSize * 2);
//                 var result = 0;
//                 try
//                 {
//                     result = socket.IOControl(IOControlCode.KeepAliveValues, inOptionValues, null);
//                 }
//                 catch (PlatformNotSupportedException)
//                 {
//                     throw new PlatformNotSupportedException("Setting TCP Keepalive Time and TCP Keepalive Interval is supported only on Windows, Mono and .NET Core 3.1+. " +
//                         "TCP keepalives can still be used on other systems but are enabled via the TcpKeepAlive option or configured globally for the machine, see the relevant docs.");
//                 }
//
//                 if (result != 0)
//                     throw new NpgsqlException($"Got non-zero value when trying to set TCP keepalive: {result}");
// #else
//             socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
//             socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, timeSeconds);
//             socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, intervalSeconds);
// #endif
}

sealed class PgPipeConnection: PgSocketConnection, IDisposable
{
    readonly SocketConnection _connection;

    PgPipeConnection(SocketConnection connection)
    {
        _connection = connection;
        Reader = new AsyncOnlyPipeReader(connection.Input);
        Writer = new AsyncOnlyPipeWriter(new PipeWriterUnflushedBytes(connection.Output));
    }

    public override IPipeReaderSyncSupport Reader { get; }
    public override IPipeWriterSyncSupport Writer { get; }
    public override bool CanBlock => false;

    public PipeShutdownKind ShutdownKind => _connection.ShutdownKind;

    const int MaxWriteBufferingOnPipe = 1024 * 1024;
    const int ResumeWriteBufferingOnPipe = MaxWriteBufferingOnPipe / 2;
    static PipeScheduler IOScheduler { get; } = PipeScheduler.Inline;
    static System.IO.Pipelines.PipeOptions DefaultSendPipeOptions { get; } =
        new(null, IOScheduler, PipeScheduler.Inline, MaxWriteBufferingOnPipe, ResumeWriteBufferingOnPipe, DefaultReaderSegmentSize, false);
    static System.IO.Pipelines.PipeOptions DefaultReceivePipeOptions { get; } =
        new(null, PipeScheduler.Inline, IOScheduler, useSynchronizationContext: false);

    public static async ValueTask<PgPipeConnection> ConnectAsync(EndPoint endPoint, CancellationToken cancellationToken = default)
    {
        var socket = CreateUnconnectedSocket(endPoint);
        await socket.ConnectAsync(endPoint, cancellationToken);
        return new(SocketConnection.Create(socket, DefaultSendPipeOptions, DefaultReceivePipeOptions));
    }

    public void Dispose()
    {
        _connection.Dispose();
    }
}

sealed class PgStreamConnection : PgSocketConnection, IDisposable, IAsyncDisposable
{
    readonly NetworkStream _stream;

    PgStreamConnection(NetworkStream stream)
    {
        _stream = stream;
        Reader = new StreamPipeReader(stream, new StreamPipeReaderOptions(bufferSize: DefaultReaderSegmentSize, useZeroByteReads: false));
        var writer = new StreamPipeWriter(stream, new StreamPipeWriterOptions(minimumBufferSize: DefaultWriterSegmentSize));
        Writer = writer;
    }

    public override IPipeReaderSyncSupport Reader { get; }
    public override IPipeWriterSyncSupport Writer { get; }
    public override bool CanBlock => true;

    public static async ValueTask<PgStreamConnection> ConnectAsync(EndPoint endPoint, CancellationToken cancellationToken = default)
    {
        var socket = CreateUnconnectedSocket(endPoint);
        await socket.ConnectAsync(endPoint, cancellationToken);
        var stream = new NetworkStream(socket, ownsSocket: true);
        return new PgStreamConnection(stream);
    }

    public static PgStreamConnection Connect(EndPoint endPoint, TimeSpan timeout = default)
    {
        var socket = CreateUnconnectedSocket(endPoint);
        ConnectWithTimeout();
        var stream = new NetworkStream(socket, ownsSocket: true);
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
        Reader.PipeReader.Complete();
        Writer.PipeWriter.Complete();
        _stream.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await Reader.PipeReader.CompleteAsync();
        await Writer.PipeWriter.CompleteAsync();
#if !NETSTANDARD2_0
        await _stream.DisposeAsync();
#else
        _stream.Dispose();
#endif
    }
}

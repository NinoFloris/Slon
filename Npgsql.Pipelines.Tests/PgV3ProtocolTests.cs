using System;
using System.Net;
using System.Threading.Channels;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol;
using NUnit.Framework;

namespace Npgsql.Pipelines.Tests;

public class PgV3ProtocolTests
{
    const string EndPoint = "127.0.0.1:5432";
    const string Username = "postgres";
    const string Password = "postgres123";
    const string Database = "postgres";

    static PgOptions PgOptions { get; } = new() { Username = Username, Password = Password, Database = Database };
    static ProtocolOptions Options { get; } = new() { ReadTimeout = TimeSpan.FromSeconds(5), ReaderSegmentSize = 1024};

    [Test]
    public async Task PipeSimpleQueryAsync()
    {
        var socket = await PgPipeConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
        var conn = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
        var command = new NpgsqlCommand(conn) { CommandText = "SELECT pg_sleep(2)" };
        await using var dataReader = await command.ExecuteReaderAsync();
        while (await dataReader.ReadAsync().ConfigureAwait(false))
        {
        }
    }

    [Test]
    public async Task StreamSimpleQueryAsync()
    {
        var socket = await PgStreamConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
        var conn = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
        var command = new NpgsqlCommand(conn) { CommandText = "SELECT pg_sleep(2)" };
        await using var dataReader = await command.ExecuteReaderAsync();
        while (await dataReader.ReadAsync().ConfigureAwait(false))
        {
        }
    }

    // [Test]
    // public void StreamSimpleQuery()
    // {
    //     try
    //     {
    //         var socket = PgStreamConnection.Connect(IPEndPoint.Parse(EndPoint));
    //         var conn = PgV3Protocol.Start(socket.Writer, socket.Reader, PgOptions, Options);
    //         conn.ExecuteQuery("SELECT pg_sleep(2)", ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty);
    //     }
    //     catch(Exception ex)
    //     {
    //         throw;
    //     }
    // }

    string _commandText = string.Empty;
    PgV3Protocol _protocol;
    const int PipelinedCommands = 10;
    readonly TaskCompletionSource<IOCompletionPair>[] _readActivations = new TaskCompletionSource<IOCompletionPair>[PipelinedCommands];
    Channel<TaskCompletionSource<IOCompletionPair>> _channel;
    Npgsql.Pipelines.NpgsqlDataReader _dataReader;
    public int NumRows { get; set; } = 1000;

    // [Test]
    // public async ValueTask PipelinesPipelined()
    // {
    //     var socket = await PgStreamConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
    //     _protocol = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
    //     _commandText = $"SELECT generate_series(1, {NumRows})";
    //     _channel = Channel.CreateUnbounded<TaskCompletionSource<IOCompletionPair>>();
    //     _dataReader = new Npgsql.Pipelines.NpgsqlDataReader();
    //
    //     var _ = Task.Run(async () =>
    //     {
    //         var reader = _channel.Reader;
    //         var conn = _protocol;
    //
    //         while (await reader.WaitToReadAsync())
    //         while (reader.TryRead(out var tcs))
    //         {
    //             var completionPair = await CommandWriter.WriteExtendedAsync(conn, new CommandInfo.Unprepared { CommandText = _commandText }, flushHint: !reader.TryPeek(out var _));
    //             tcs.SetResult(completionPair);
    //         }
    //     });
    //
    //     var activations = _readActivations;
    //     var writer = _channel.Writer;
    //     for (var i = 0; i < activations.Length; i++)
    //         writer.TryWrite(activations[i] = new TaskCompletionSource<IOCompletionPair>());
    //
    //     var dataReader = _dataReader;
    //     for (var i = 0; i < activations.Length; i++)
    //     {
    //         var completionPair = await activations[i].Task;
    //         await using var reader = dataReader;
    //         await reader.IntializeAsync(completionPair, CommandBehavior.Default, CancellationToken.None);
    //         while (await reader.ReadAsync())
    //         {
    //         }
    //     }
    // }

    [Test]
    public async ValueTask PipeliningTest()
    {
        var socket = await PgStreamConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
        var conn = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
        var NumRows = 1000;
        var command = new NpgsqlCommand(conn) { CommandText = $"SELECT generate_series(1, {NumRows})" };
        const int Pipelined = 1000;
        var outer = 10;

        for (int j = 0; j < outer; j++)
        {
            var commands = new Task<NpgsqlDataReader>[Pipelined];

            for (int i = 0; i < Pipelined; i++)
            {
                commands[i] = command.ExecuteReaderAsync();
            }

            for (int i = 0; i < Pipelined; i++)
            {
                await using var reader = await commands[i];
                while (await reader.ReadAsync())
                {
                }
            }
        }
    }
}

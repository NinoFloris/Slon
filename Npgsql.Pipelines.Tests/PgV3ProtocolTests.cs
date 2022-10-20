using System;
using System.Net;
using System.Threading.Channels;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol;
using Npgsql.Pipelines.Protocol.PgV3;
using NUnit.Framework;

namespace Npgsql.Pipelines.Tests;

public class PgV3ProtocolTests
{
    const string EndPoint = "127.0.0.1:5432";
    const string Username = "postgres";
    const string Password = "postgres123";
    const string Database = "postgres";

    static PgV3ProtocolOptions ProtocolOptions { get; } = new() { ReadTimeout = TimeSpan.FromSeconds(5)};
    static NpgsqlDataSourceOptions Options { get; } = new()
    {
        EndPoint = IPEndPoint.Parse(EndPoint),
        Username = Username,
        Password = Password,
        Database = Database,
        PoolSize = 10
    };

    [Test]
    public async Task PipeSimpleQueryAsync()
    {
        var dataSource = new NpgsqlDataSource(Options, ProtocolOptions);
        var command = new NpgsqlCommand("SELECT pg_sleep(2)", new NpgsqlConnection(dataSource));
        await using var dataReader = await command.ExecuteReaderAsync();
        while (await dataReader.ReadAsync().ConfigureAwait(false))
        {
        }
    }

    [Test]
    public async Task StreamSimpleQueryAsync()
    {
        var dataSource = new NpgsqlDataSource(Options, ProtocolOptions);
        var connection = new NpgsqlConnection(dataSource);
        await connection.OpenAsync();
        var command = new NpgsqlCommand("SELECT pg_sleep(2)", connection);
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
        const int NumRows = 1000;
        const int Commands = 1000;
        NpgsqlCommand command;

        var commandText = $"SELECT generate_series(1, {NumRows})";

        var dataSource = new NpgsqlDataSource(Options with { PoolSize = 10 }, ProtocolOptions);
        var conn = new NpgsqlConnection(dataSource);
        await conn.OpenAsync();
        // command = dataSource.CreateCommand(commandText);
        command = new NpgsqlCommand(commandText, conn) { CommandText = commandText };

        // var dataSource2 = new NpgsqlDataSource(Options with { PoolSize = 1 }, ProtocolOptions);
        // command = dataSource2.CreateCommand(_commandText);

        var outer = 1;
        for (int j = 0; j < outer; j++)
        {
            var readerTasks = new Task<NpgsqlDataReader>[Commands];
            for (var i = 0; i < readerTasks.Length; i++)
            {
                readerTasks[i] = command.ExecuteReaderAsync();
            }

            for (var i = 0; i < readerTasks.Length; i++)
            {
                await using var reader = await readerTasks[i];
                while (await reader.ReadAsync())
                {
                }
            }
        }
    }
}

using System;
using System.Net;
using System.Threading.Channels;
using System.Threading.Tasks;
using Xunit;
using Slon.Protocol;
using Slon.Protocol.PgV3;

namespace Slon.FunctionalTests;

[Collection("Database")]
public class PgV3ProtocolTests
{
    const string EndPoint = "127.0.0.1:5432";
    const string Username = "postgres";
    const string Password = "postgres123";
    const string Database = "postgres";

    static PgV3ProtocolOptions ProtocolOptions { get; } = new() { ReadTimeout = TimeSpan.FromSeconds(5)};
    static SlonDataSourceOptions Options { get; } = new()
    {
        EndPoint = IPEndPoint.Parse(EndPoint),
        Username = Username,
        Password = Password,
        Database = Database,
        PoolSize = 10
    };

    [Fact]
    public async Task SimpleQueryAsync()
    {
        var dataSource = new SlonDataSource(Options, ProtocolOptions);
        await using var conn = new SlonConnection(dataSource);
        await conn.OpenAsync();
        var command = new SlonCommand("SELECT pg_sleep(2)", conn);
        await using var dataReader = await command.ExecuteReaderAsync();
        while (await dataReader.ReadAsync().ConfigureAwait(false))
        {
        }
    }

    string _commandText = string.Empty;
    // PgV3Protocol _protocol = null!;
    const int PipelinedCommands = 10;
    readonly TaskCompletionSource<IOCompletionPair>[] _readActivations = new TaskCompletionSource<IOCompletionPair>[PipelinedCommands];
    // Channel<TaskCompletionSource<IOCompletionPair>> _channel = null!;
    // SlonDataReader _dataReader = null!;
    public int NumRows { get; set; } = 1000;

    // [Test]
    // public async ValueTask PipelinesPipelined()
    // {
    //     var socket = await PgStreamConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
    //     _protocol = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
    //     _commandText = $"SELECT generate_series(1, {NumRows})";
    //     _channel = Channel.CreateUnbounded<TaskCompletionSource<IOCompletionPair>>();
    //     _dataReader = new Slon.SlonDataReader();
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

    [Fact]
    public async ValueTask PipeliningTest()
    {
        const int NumRows = 1000;
        const int Commands = 1000;
        SlonCommand command;

        var commandText = $"SELECT generate_series(1, {NumRows})";

        var dataSource = new SlonDataSource(Options with { PoolSize = 10 }, ProtocolOptions);
        var conn = new SlonConnection(dataSource);
        await conn.OpenAsync();
        // command = dataSource.CreateCommand(commandText);
        command = new SlonCommand(commandText, conn) { CommandText = commandText };

        // var dataSource2 = new SlonDataSource(Options with { PoolSize = 1 }, ProtocolOptions);
        // command = dataSource2.CreateCommand(_commandText);

        var outer = 1;
        for (int j = 0; j < outer; j++)
        {
            var readerTasks = new Task<SlonDataReader>[Commands];
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

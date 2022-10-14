using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Channels;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Npgsql.Pipelines.Protocol;

namespace Npgsql.Pipelines.Benchmark
{
    [SimpleJob(targetCount: 20)]
    [MemoryDiagnoser(true)]
    public class Benchmarks
    {
        const string EndPoint = "127.0.0.1:5432";
        const string Username = "postgres";
        const string Password = "postgres123";
        const string Database = "postgres";

        const string ConnectionString = $"Server={EndPoint};User ID={Username};Password={Password};Database=postgres;SSL Mode=Disable;Pooling=false;Max Auto Prepare=0;";
        const string ConnectionString2 = $"Server={EndPoint};User ID={Username};Password={Password};Database=postgres;SSL Mode=Disable;Pooling=true;MaxPoolSize=1;Max Auto Prepare=0;Multiplexing=true";
        static PgOptions PgOptions { get; } = new() { Username = Username, Password = Password, Database = Database };
        static ProtocolOptions Options { get; } = new() { ReadTimeout = TimeSpan.FromSeconds(5) };

        string _commandText = string.Empty;
        PgV3Protocol _protocol;

        Npgsql.NpgsqlCommand Command;
        NpgsqlConnection _conn;
        Channel<NpgsqlCommand> _channel;
        ChannelWriter<NpgsqlCommand> _channelWriter;

        [GlobalSetup(Targets = new[] { nameof(Pipelines), nameof(PipelinesPipelined) })]
        public async ValueTask SetupPipelines()
        {
            var socket = await PgStreamConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
            _protocol = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
            _commandText = $"SELECT generate_series(1, {NumRows})";
            const int MultiplexingCommandChannelBound = 4096;
            _channel = Channel.CreateBounded<NpgsqlCommand>(new BoundedChannelOptions(MultiplexingCommandChannelBound)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true
            });
            _channelWriter = _channel.Writer;

            var _ = Task.Run(async () =>
            {
                var reader = _channel.Reader;
                var conn = _protocol;

                while (await reader.WaitToReadAsync())
                while (reader.TryRead(out var command))
                {
                    var completionPair = CommandWriter.WriteExtendedAsync(conn, command, flushHint: !reader.TryPeek(out var _));
                    command.CompleteMultiplexingOperation(completionPair);
                }
            });
        }

        [GlobalSetup(Targets = new []{ nameof(Npgsql)})]
        public async ValueTask SetupNpgsql()
        {
            _conn = new NpgsqlConnection(ConnectionString);
            await _conn.OpenAsync();
            Command = new Npgsql.NpgsqlCommand($"SELECT generate_series(1, {NumRows})", _conn);
        }

        [GlobalSetup(Targets = new []{ nameof(NpgsqlPipelined) })]
        public async ValueTask SetupNpgsqlMultiplexing()
        {
            _conn = new NpgsqlConnection(ConnectionString2);
            await _conn.OpenAsync();
            _commandText = $"SELECT generate_series(1, {NumRows})";
        }


        // [Benchmark(Baseline = true)]
        public async ValueTask Npgsql()
        {
            await using var reader = await Command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {

            }
        }

        [Params(1000)]
        public int NumRows { get; set; }
        const int PipelinedCommandsConst = 1000;
        [Params(PipelinedCommandsConst)]
        public int PipelinedCommands { get; set; }

        [Benchmark(OperationsPerInvoke = PipelinedCommandsConst)]
        public async ValueTask PipelinesPipelined()
        {
            var command = new NpgsqlCommand(_protocol) { CommandText = _commandText };
            var readerTasks = new Task<NpgsqlDataReader>[PipelinedCommands];
            for (var i = 0; i < readerTasks.Length; i++)
            {
                readerTasks[i] = command.ExecuteReaderAsync();
            }

            for (var i = 0; i < readerTasks.Length; i++)
            {
                await using var reader = await readerTasks[i];
            }
        }

        [Benchmark(OperationsPerInvoke = PipelinedCommandsConst, Baseline = true)]
        public async ValueTask NpgsqlPipelined()
        {
            var readerTasks = new Task<Npgsql.NpgsqlDataReader>[PipelinedCommands];
            for (var i = 0; i < readerTasks.Length; i++)
            {
                readerTasks[i] = new Npgsql.NpgsqlCommand(_commandText, _conn).ExecuteReaderAsync();
            }

            for (var i = 0; i < readerTasks.Length; i++)
            {
                await using var reader = await readerTasks[i];
            }
        }

        // // [Benchmark]
        // public async ValueTask Pipelines()
        // {
        //     var conn = _protocol;
        //     var activation = await conn.ExecuteQueryAsync(_commandText, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty);
        //     await activation.Task;
        //     var dataReader = new Npgsql.Pipelines.NpgsqlDataReader(conn);
        //     await dataReader.IntializeAsync(activation);
        //     while (await dataReader.ReadAsync())
        //     {
        //     }
        //     await conn.ReadMessageAsync<ReadyForQuery>();
        //     activation.Complete();
        // }

    }
}

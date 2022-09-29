using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Channels;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Npgsql.Pipelines.MiscMessages;
using Npgsql.Pipelines.QueryMessages;

namespace Npgsql.Pipelines.Benchmark
{
    [SimpleJob(targetCount: 20)]
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

        [Params(1000)]
        public int NumRows { get; set; }

        string _commandText = string.Empty;
        PgV3Protocol _protocol;

        NpgsqlCommand Command;

        [GlobalSetup(Targets = new[] { nameof(Pipelines), nameof(PipelinesPipelined) })]
        public async ValueTask SetupPipelines()
        {
            var socket = await PgStreamConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
            _protocol = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
            _commandText = $"SELECT generate_series(1, {NumRows})";
            _channel = Channel.CreateUnbounded<bool>();

            var _ = Task.Run(async () =>
            {
                var reader = _channel.Reader;
                var conn = _protocol;

                while (reader.TryRead(out var _) || await reader.WaitToReadAsync())
                {
                    await conn.ExecuteQueryAsync(_commandText, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty, reader.TryPeek(out var _));
                }
            });
        }

        [GlobalSetup(Targets = new []{ nameof(Npgsql)})]
        public async ValueTask SetupNpgsql()
        {
            _conn = new NpgsqlConnection(ConnectionString);
            await _conn.OpenAsync();
            Command = new NpgsqlCommand($"SELECT generate_series(1, {NumRows})", _conn);
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

        [Benchmark(OperationsPerInvoke = PipelinedCommands)]
        public async ValueTask NpgsqlPipelined()
        {
            var readerTasks = new Task<NpgsqlDataReader>[PipelinedCommands];
            for (var i = 0; i < readerTasks.Length; i++)
            {
                readerTasks[i] = new NpgsqlCommand(_commandText, _conn).ExecuteReaderAsync();
            }

            for (var i = 0; i < readerTasks.Length; i++)
            {
                await using var reader = await readerTasks[i];
                while (await reader.ReadAsync()) // put a breakpoint here and run test in the debugger
                {

                }
            }
        }

        // [Benchmark]
        public async ValueTask Pipelines()
        {
            var conn = _protocol;
            var activation = await conn.ExecuteQueryAsync(_commandText, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty);
            await activation.Task;
            await conn.ReadMessageAsync<ParseComplete>();
            await conn.ReadMessageAsync<BindComplete>();
            using var description = await conn.ReadMessageAsync(new RowDescription(conn._fieldDescriptionPool));

            var dataReader = new DataReader(conn, description);
            while (await dataReader.ReadAsync())
            {
                // var i = await dataReader.GetFieldValueAsync<int>().ConfigureAwait(false);;
                // i = i;
            }
            await conn.ReadMessageAsync<ReadyForQuery>();
            activation.Complete();
        }


        const int PipelinedCommands = 1000;
        readonly ReadActivation[] _readActivations = new ReadActivation[PipelinedCommands];
        NpgsqlConnection _conn;
        Channel<bool> _channel;

        [Benchmark(OperationsPerInvoke = PipelinedCommands)]
        public async ValueTask PipelinesPipelined()
        {
        var activations = _readActivations;
        var conn = _protocol;
        var writer = _channel.Writer;
        for (var i = 0; i < activations.Length; i++)
        {
            writer.TryWrite(true);
            // conn.ExecuteQueryAsync(_commandText, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty, i < activations.Length - 1);
        }

        for (var i = 0; i < activations.Length; i++)
        {
            // var activation = activations[i];
            // await activation.Task;
            await conn.WaitForDataAsync(50);
            await conn.ReadMessageAsync<InlinedReader>();
            // await conn.ReadMessageAsync<ParseComplete>();
            // await conn.ReadMessageAsync<BindComplete>();
            // using var description = await conn.ReadMessageAsync(new RowDescription(conn._fieldDescriptionPool));
            // var dataReader = new DataReader(conn, description);
            // while (await dataReader.ReadAsync())
            // {
            //     // var i = await dataReader.GetFieldValueAsync<int>().ConfigureAwait(false);;
            //     // i = i;
            // }
            // await conn.ReadMessageAsync<ReadyForQuery>();
            // activation.Complete();
        }
        }

        struct InlinedReader: IBackendMessage
        {
            int _state;
            public ReadStatus Read(ref MessageReader reader)
            {
                switch (_state)
                {
                    case 1: goto state1;
                    case 2: goto state2;
                    case 3: goto state3;
                    case 4: goto state4;
                }

                if (!reader.MoveNextAndIsExpected(BackendCode.ParseComplete, out var status, ensureBuffered: true))
                    return status;

                _state = 1;
                state1:
                if (!reader.MoveNextAndIsExpected(BackendCode.BindComplete, out status, ensureBuffered: true))
                    return status;

                _state = 2;
                state2:
                status = new RowDescription().Read(ref reader);
                if (status != ReadStatus.Done)
                    return status;

                _state = 3;
                state3:

                if (!reader.MoveNext())
                    return ReadStatus.NeedMoreData;

                if (!reader.SkipSimilar(BackendCode.DataRow, out status))
                    return status;

                if (!reader.IsExpected(BackendCode.CommandComplete, out status, ensureBuffered: true))
                    return status;

                _state = 4;
                state4:

                if (!reader.MoveNextAndIsExpected(BackendCode.ReadyForQuery, out status, ensureBuffered: true))
                    return status;

                reader.ConsumeCurrent();
                return ReadStatus.Done;
            }
        }
    }
}

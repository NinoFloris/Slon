using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Npgsql.Pipelines.MiscMessages;
using Npgsql.Pipelines.QueryMessages;

namespace Npgsql.Pipelines.Benchmark
{
    public class Benchmarks
    {
        const string EndPoint = "127.0.0.1:5432";
        const string Username = "postgres";
        const string Password = "postgres123";
        const string Database = "postgres";

        const string ConnectionString = $"Server={EndPoint};User ID={Username};Password={Password};Database=postgres;SSL Mode=Disable;Pooling=false;Max Auto Prepare=0;";

        static PgOptions PgOptions { get; } = new() { Username = Username, Password = Password, Database = Database };
        static ProtocolOptions Options { get; } = new() { ReadTimeout = TimeSpan.FromSeconds(5) };

        [Params(1)]
        public int NumRows { get; set; }

        string _commandText = string.Empty;
        PgV3Protocol _protocol;

        NpgsqlCommand Command;

        [GlobalSetup(Targets = new[] { nameof(ReadPipelines) })]
        public async ValueTask SetupNpgsqlPipelines()
        {
            var socket = await PgStreamConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
            _protocol = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
            _commandText = $"SELECT generate_series(1, {NumRows})";
        }

        [GlobalSetup(Target = nameof(ReadNpgsql))]
        public async ValueTask SetupNpgsql()
        {
            var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync();
            Command = new NpgsqlCommand($"SELECT generate_series(1, {NumRows})", conn);
        }

        // [Benchmark(Baseline = true)]
        public async ValueTask ReadNpgsql()
        {
            await using var reader = await Command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {

            }
        }

        const int PipelinedCommands = 1000;

        [Benchmark(OperationsPerInvoke = PipelinedCommands)]
        public async ValueTask ReadPipelines()
        {
            for (int i = 0; i < PipelinedCommands; i++)
            {
                await _protocol.ExecuteQueryAsync(_commandText, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty);
            }

            for (int i = 0; i < PipelinedCommands; i++)
            {
                await _protocol.ReadMessageAsync<ParseComplete>(CancellationToken.None).ConfigureAwait(false);
                await _protocol.ReadMessageAsync<BindComplete>().ConfigureAwait(false);
                using var description = await _protocol.ReadMessageAsync(new RowDescription(_protocol._fieldDescriptionPool)).ConfigureAwait(false);
                var dataReader = new DataReader(_protocol, description);
                while (await dataReader.ReadAsync().ConfigureAwait(false))
                {
                    // var i = await dataReader.GetFieldValueAsync<int>().ConfigureAwait(false);;
                    // i = i;
                }

                await _protocol.ReadMessageAsync<ReadyForQuery>().ConfigureAwait(false);
            }

        }

    }
}

﻿using System;
using System.Collections.Generic;
using System.Net;
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

        static PgOptions PgOptions { get; } = new() { Username = Username, Password = Password, Database = Database };
        static ProtocolOptions Options { get; } = new() { ReadTimeout = TimeSpan.FromSeconds(5) };

        [Params(1)]
        public int NumRows { get; set; }

        string _commandText = string.Empty;
        PgV3Protocol _protocol;

        NpgsqlCommand Command;

        [GlobalSetup(Targets = new[] { nameof(ReadPipelines), nameof(ReadPipelinesPipelined) })]
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

        [Benchmark(Baseline = true)]
        public async ValueTask ReadNpgsql()
        {
            await using var reader = await Command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {

            }
        }

        [Benchmark]
        public async ValueTask ReadPipelines()
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

        [Benchmark(OperationsPerInvoke = PipelinedCommands)]
        public async ValueTask ReadPipelinesPipelined()
        {
        var activations = _readActivations;
        var conn = _protocol;
        for (var i = 0; i < activations.Length; i++)
        {
            activations[i] = await conn.ExecuteQueryAsync(_commandText, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty, i < activations.Length - 1);
        }

        for (var i = 0; i < activations.Length; i++)
        {
            var activation = activations[i];
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
        }

    }
}
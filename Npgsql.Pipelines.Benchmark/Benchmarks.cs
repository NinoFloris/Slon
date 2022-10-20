using System;
using System.Net;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using Npgsql.Pipelines.Protocol.PgV3;

namespace Npgsql.Pipelines.Benchmark
{
    [Config(typeof(Config))]
    public class Benchmarks
    {
        class Config : ManualConfig
        {
            public Config()
            {
                Add(new SimpleJobAttribute(targetCount: 20).Config);
                AddDiagnoser(MemoryDiagnoser.Default);
                AddColumn(new TagColumn("Connections", name => (name.EndsWith("Pipelined") ? 1 : Connections).ToString()));
            }
        }

        const string EndPoint = "127.0.0.1:5432";
        const string Username = "postgres";
        const string Password = "postgres123";
        const string Database = "postgres";

        const string ConnectionString = $"Server={EndPoint};User ID={Username};Password={Password};Database=postgres;SSL Mode=Disable;Pooling=false;Max Auto Prepare=0;";
        const string ConnectionString2 = $"Server={EndPoint};User ID={Username};Password={Password};Database=postgres;SSL Mode=Disable;Pooling=true;MinPoolSize=1;MaxPoolSize=1;Max Auto Prepare=0;Multiplexing=true";
        const string ConnectionString3 = $"Server={EndPoint};User ID={Username};Password={Password};Database=postgres;SSL Mode=Disable;Pooling=true;MinPoolSize=10;MaxPoolSize=10;Max Auto Prepare=0;Multiplexing=true";
        static PgV3ProtocolOptions ProtocolOptions { get; } = new() { ReadTimeout = TimeSpan.FromSeconds(5)};
        static NpgsqlDataSourceOptions Options { get; } = new()
        {
            EndPoint = IPEndPoint.Parse(EndPoint),
            Username = Username,
            Password = Password,
            Database = Database,
            PoolSize = Connections
        };


        string _commandText = string.Empty;

        Npgsql.NpgsqlConnection _npgsqlConn;

        NpgsqlConnection _conn;
        NpgsqlCommand _pipeliningCommand;
        NpgsqlCommand _multiplexedPipeliningCommand;
        NpgsqlCommand _multiplexingCommand;

        [GlobalSetup(Targets = new[] { nameof(PipelinesPipelined), nameof(PipelinesMultiplexingPipelined), nameof(PipelinesMultiplexing) })]
        public async ValueTask SetupPipelines()
        {
            _commandText = $"SELECT generate_series(1, {RowsPer})";

            // Pipelining
            var dataSource = new NpgsqlDataSource(Options with {PoolSize = 1}, ProtocolOptions);
            _conn = new NpgsqlConnection(dataSource);
            await _conn.OpenAsync();
            _pipeliningCommand = new NpgsqlCommand(_commandText, _conn);

            var dataSource2 = new NpgsqlDataSource(Options with {PoolSize = 1}, ProtocolOptions);
            _multiplexedPipeliningCommand = dataSource2.CreateCommand(_commandText);

            // Multiplexing
            var dataSource3 = new NpgsqlDataSource(Options, ProtocolOptions);
            _multiplexingCommand = dataSource3.CreateCommand(_commandText);
        }

        [GlobalSetup(Targets = new []{ nameof(NpgsqlMultiplexingPipelined), nameof(NpgsqlMultiplexing) })]
        public async ValueTask SetupNpgsqlMultiplexing()
        {
            var builder = new NpgsqlConnectionStringBuilder(ConnectionString2);
            builder.MinPoolSize = Connections;
            builder.MaxPoolSize = Connections;
            _npgsqlConn = new Npgsql.NpgsqlConnection(builder.ToString());
            await _npgsqlConn.OpenAsync();
            _commandText = $"SELECT generate_series(1, {RowsPer})";
        }

        public const int Connections = 10;

        [Params(1000)]
        public int RowsPer { get; set; }
        [Params(1000, 10000)]
        public int Commands { get; set; }

        [Benchmark]
        public async ValueTask PipelinesPipelined()
        {
            var pipeliningCommand = _pipeliningCommand;
            var readerTasks = new Task<NpgsqlDataReader>[Commands];
            for (var i = 0; i < readerTasks.Length; i++)
            {
                readerTasks[i] = pipeliningCommand.ExecuteReaderAsync();
            }

            for (var i = 0; i < readerTasks.Length; i++)
            {
                await using var reader = await readerTasks[i];
                while (await reader.ReadAsync())
                {
                }
            }
        }

        [Benchmark]
        public async ValueTask PipelinesMultiplexingPipelined()
        {
            var multiplexingCommand = _multiplexedPipeliningCommand;
            var readerTasks = new Task<NpgsqlDataReader>[Commands];
            for (var i = 0; i < readerTasks.Length; i++)
            {
                readerTasks[i] = multiplexingCommand.ExecuteReaderAsync();
            }

            for (var i = 0; i < readerTasks.Length; i++)
            {
                await using var reader = await readerTasks[i];
                while (await reader.ReadAsync())
                {
                }
            }
        }

        [Benchmark]
        public async ValueTask NpgsqlMultiplexingPipelined()
        {
            var readerTasks = new Task<Npgsql.NpgsqlDataReader>[Commands];
            for (var i = 0; i < readerTasks.Length; i++)
            {
                var conn = new Npgsql.NpgsqlConnection(ConnectionString2);
                await conn.OpenAsync();
                readerTasks[i] = new Npgsql.NpgsqlCommand(_commandText, conn).ExecuteReaderAsync();
            }

            for (var i = 0; i < readerTasks.Length; i++)
            {
                await using var reader = await readerTasks[i];
                while (await reader.ReadAsync())
                {
                }
            }
        }

        [Benchmark]
        public async ValueTask PipelinesMultiplexing()
        {
            var multiplexingCommand = _multiplexingCommand;
            var readerTasks = new Task<NpgsqlDataReader>[Commands];
            for (int i = 0; i < readerTasks.Length; i++)
            {
                readerTasks[i] = multiplexingCommand.ExecuteReaderAsync();
            }

            for (int i = 0; i < Commands; i++)
            {
                await using var reader = await readerTasks[i];
                while (await reader.ReadAsync())
                {
                }
            }
        }

        [Benchmark(Baseline = true)]
        public async ValueTask NpgsqlMultiplexing()
        {
            var readerTasks = new Task<Npgsql.NpgsqlDataReader>[Commands];
            for (var i = 0; i < readerTasks.Length; i++)
            {
                var conn = new Npgsql.NpgsqlConnection(ConnectionString3);
                await conn.OpenAsync();
                readerTasks[i] = new Npgsql.NpgsqlCommand(_commandText, conn).ExecuteReaderAsync();
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

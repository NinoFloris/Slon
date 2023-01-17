using System;
using System.Net;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol.PgV3;
using NUnit.Framework;

namespace Npgsql.Pipelines.Tests;

public class PreparationTests
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
        var command = dataSource.CreateCommand("SELECT 1");
        command.Prepare();
        await using var dataReader = await command.ExecuteReaderAsync();
        while (await dataReader.ReadAsync().ConfigureAwait(false))
        {
        }
    }

}

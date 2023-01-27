using System;
using System.Collections.Concurrent;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Slon.Protocol.PgV3;
using Xunit;

namespace Slon.FunctionalTests;

[CollectionDefinition("Database")]
public class DatabaseService : ICollectionFixture<DatabaseService>
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

    readonly ConcurrentDictionary<SlonDataSourceOptions, SlonDataSource> _dataSources = new();

    SlonDataSource GetOrAddDataSource(SlonDataSourceOptions? options = null)
        => _dataSources.GetOrAdd(options ?? Options, options => new SlonDataSource(options, ProtocolOptions));

    internal ValueTask<SlonConnection> OpenConnectionAsync(SlonDataSourceOptions? options = null, CancellationToken cancellationToken = default)
        => GetOrAddDataSource(options).OpenConnectionAsync(cancellationToken);

    internal SlonCommand CreateCommand(string? commandText = null, SlonDataSourceOptions? options = null)
        => GetOrAddDataSource(options).CreateCommand(commandText);
}

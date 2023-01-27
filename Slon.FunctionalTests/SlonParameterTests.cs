using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;

namespace Slon.FunctionalTests;

[Collection("Database")]
public record SlonParameterTests(DatabaseService DbService)
{
    [Fact]
    public async Task Test()
    {
        const int sleepSeconds = 2;
        var conn = await DbService.OpenConnectionAsync();
        var sw = Stopwatch.StartNew();
        var command = new SlonCommand("SELECT pg_sleep($1)", conn);
        await using var dataReader = await command.ExecuteReaderAsync(new SlonParameterCollection { sleepSeconds });
        while (await dataReader.ReadAsync())
        {
        }
        // We can't read a column yet so we use an observable side effect.
        Assert.InRange(sw.ElapsedMilliseconds, sleepSeconds * 1000, Int64.MaxValue);
    }
}

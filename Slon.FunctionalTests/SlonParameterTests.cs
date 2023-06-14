using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Slon.Pg;
using Xunit;

namespace Slon.FunctionalTests;

[Collection("Database")]
public class SlonParameterTests(DatabaseService DbService)
{
    [Fact]
    public async Task SendOneInt()
    {
        const int sleepSeconds = 1;
        var sw = Stopwatch.StartNew();
        await using var dataReader = await DbService
            .CreateCommand("SELECT pg_sleep($1)")
            .ExecuteReaderAsync(new SlonParameterCollection { sleepSeconds });
        while (await dataReader.ReadAsync()) { }

        // We can't read a column yet so we use an observable side effect.
        Assert.InRange(sw.ElapsedMilliseconds, sleepSeconds * 1000, Int64.MaxValue);
    }

    [Fact]
    public async Task SendOneIntExpectTwoInts()
    {
        const int sleepSeconds = 1;
        await Assert.ThrowsAsync<PostgresException>(async () =>
        {
            await using var dataReader = await DbService
                .CreateCommand("SELECT pg_sleep($1), pg_sleep($2)")
                .ExecuteReaderAsync(new SlonParameterCollection { sleepSeconds });
        });
        Assert.True(true);
    }

    [Fact]
    public async Task SendTwoInts()
    {
        const int sleepSeconds = 1;
        var sw = Stopwatch.StartNew();
        var dataReader = await DbService
            .CreateCommand("SELECT pg_sleep($1), pg_sleep($2)")
            .ExecuteReaderAsync(new SlonParameterCollection { sleepSeconds, sleepSeconds });

        await using var dr = dataReader;
        while (await dataReader.ReadAsync()) { }

        // We can't read a column yet so we use an observable side effect.
        Assert.InRange(sw.ElapsedMilliseconds, sleepSeconds * 2 * 1000, Int64.MaxValue);
    }
}

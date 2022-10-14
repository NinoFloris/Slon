using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol;
using NUnit.Framework;

namespace Npgsql.Pipelines.Tests;

public class PgV3ProtocolTests
{
    const string EndPoint = "127.0.0.1:5432";
    const string Username = "postgres";
    const string Password = "postgres123";
    const string Database = "postgres";

    static PgOptions PgOptions { get; } = new() { Username = Username, Password = Password, Database = Database };
    static ProtocolOptions Options { get; } = new() { ReadTimeout = TimeSpan.FromSeconds(5), ReaderSegmentSize = 1024};

    [Test]
    public async Task PipeSimpleQueryAsync()
    {
        try
        {
            var socket = await PgPipeConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
            var conn = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
            await conn.ExecuteQueryAsync("SELECT pg_sleep(2)", ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty);
            var dataReader = new NpgsqlDataReader(conn);
            while (await dataReader.ReadAsync().ConfigureAwait(false))
            {
            }
            await conn.ReadMessageAsync<ReadyForQuery>().ConfigureAwait(false);
        }
        catch(Exception ex)
        {
            throw;
        }
    }

    [Test]
    public async Task StreamSimpleQueryAsync()
    {
        try
        {
            var socket = await PgStreamConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
            var conn = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
            await conn.ExecuteQueryAsync("SELECT pg_sleep(2)", ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty);
        }
        catch(Exception ex)
        {
            throw;
        }
    }

    [Test]
    public void StreamSimpleQuery()
    {
        try
        {
            var socket = PgStreamConnection.Connect(IPEndPoint.Parse(EndPoint));
            var conn = PgV3Protocol.Start(socket.Writer, socket.Reader, PgOptions, Options);
            conn.ExecuteQuery("SELECT pg_sleep(2)", ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty);
        }
        catch(Exception ex)
        {
            throw;
        }
    }

    [Test]
    public async ValueTask PipeliningTest()
    {
        var socket = await PgStreamConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
        var conn = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
        var NumRows = 1000;
        const int Pipelined = 1000;
        var outer = 1;

        var dataReader = new NpgsqlDataReader(conn);

        for (int j = 0; j < outer; j++)
        {
            var activations = new ReadActivation[Pipelined];

            for (int i = 0; i < Pipelined; i++)
            {
                activations[i] = await conn.ExecuteQueryAsync($"SELECT generate_series(1, {NumRows})", ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty, i < activations.Length - 1);
            }

            for (int i = 0; i < Pipelined; i++)
            {
                await dataReader.IntializeAsync(activations[i]);
                while (await dataReader.ReadAsync().ConfigureAwait(false))
                {
                    // var i = await dataReader.GetFieldValueAsync<int>().ConfigureAwait(false);;
                    // i = i;
                }
                await conn.ReadMessageAsync<ReadyForQuery>().ConfigureAwait(false);
                dataReader.Dispose();
            }
        }
    }
}

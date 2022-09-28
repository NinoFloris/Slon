using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.MiscMessages;
using Npgsql.Pipelines.QueryMessages;
using NUnit.Framework;

namespace Npgsql.Pipelines.Tests;

public class PgV3ProtocolTests
{
    const string EndPoint = "127.0.0.1:5432";
    const string Username = "postgres";
    const string Password = "postgres123";
    const string Database = "postgres";

    static PgOptions PgOptions { get; } = new() { Username = Username, Password = Password, Database = Database };
    static ProtocolOptions Options { get; } = new() { ReadTimeout = TimeSpan.FromSeconds(5) };

    [Test]
    public async Task PipeSimpleQueryAsync()
    {
        try
        {
            var socket = await PgPipeConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
            var conn = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
            await conn.ExecuteQueryAsync("SELECT pg_sleep(2)", ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty);

            await conn.ReadMessageAsync<ParseComplete>(CancellationToken.None).ConfigureAwait(false);
            await conn.ReadMessageAsync<BindComplete>().ConfigureAwait(false);
            using var description = await conn.ReadMessageAsync(new RowDescription(conn._fieldDescriptionPool)).ConfigureAwait(false);
            var dataReader = new DataReader(conn, description);
            while (await dataReader.ReadAsync().ConfigureAwait(false))
            {
                // var i = await dataReader.GetFieldValueAsync<int>().ConfigureAwait(false);;
                // i = i;
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
        // await Task.Delay(10000);
        var socket = await PgStreamConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
        var conn = await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, PgOptions, Options);
        var NumRows = 1;
        const int Pipelined = 1000;
        var outer = 10;

        for (int j = 0; j < outer; j++)
        {
            var activations = new ReadActivation[Pipelined];

            for (int i = 0; i < Pipelined; i++)
            {
                activations[i] = await conn.ExecuteQueryAsync($"SELECT generate_series(1, {NumRows})", ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>.Empty, i < activations.Length - 1);
            }

            for (int i = 0; i < Pipelined; i++)
            {
                var activation = activations[i];
                await activation.Task.ConfigureAwait(false);
                await conn.ReadMessageAsync<ParseComplete>(CancellationToken.None).ConfigureAwait(false);
                await conn.ReadMessageAsync<BindComplete>().ConfigureAwait(false);
                using var description = await conn.ReadMessageAsync(new RowDescription(conn._fieldDescriptionPool)).ConfigureAwait(false);
                var dataReader = new DataReader(conn, description);
                while (await dataReader.ReadAsync().ConfigureAwait(false))
                {
                    // var i = await dataReader.GetFieldValueAsync<int>().ConfigureAwait(false);;
                    // i = i;
                }
                await conn.ReadMessageAsync<ReadyForQuery>().ConfigureAwait(false);
                activation.Complete();
            }
        }
    }
}

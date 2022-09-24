using System;
using System.Net;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Npgsql.Pipelines.Tests;

public class PgV3ProtocolTests
{
    const string EndPoint = "127.0.0.1:5432";
    const string Username = "postgres";
    const string Password = "postgres123";
    const string Database = "postgres";

    [Test]
    public async Task PipeSimpleQueryAsync()
    {
        try
        {
            var socket = await PgPipeConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
            await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, new ConnectionOptions { Username = Username, Password = Password, Database = Database });
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
            await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, new ConnectionOptions { Username = Username, Password = Password, Database = Database });
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
            PgV3Protocol.Start(socket.Writer, socket.Reader, new ConnectionOptions { Username = Username, Password = Password, Database = Database });
        }
        catch(Exception ex)
        {
            throw;
        }
    }
}

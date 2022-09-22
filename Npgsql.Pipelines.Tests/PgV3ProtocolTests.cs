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
    public async Task SimpleQueryEmptyQuery()
    {
        try
        {
            var socket = await PgSocketConnection.ConnectAsync(IPEndPoint.Parse(EndPoint));
            await PgV3Protocol.StartAsync(socket.Writer, socket.Reader, new ConnectionOptions { Username = Username, Password = Password, Database = Database });
        }
        catch(Exception ex)
        {
            throw;
        }
    }
}

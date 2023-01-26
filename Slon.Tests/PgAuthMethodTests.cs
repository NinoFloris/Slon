using System;
using NUnit.Framework;
using Slon.Protocol.Pg;
using Slon.Protocol.PgV3;

namespace Slon.Tests;

public class PgAuthMethodTests
{
    static ReadOnlySpan<byte> Salt => new[]
    {
        (byte)'a', (byte)'b', (byte)'c', (byte)'d'
    };

    [Test]
    public void Md5StringValid()
    {
        // From https://www.postgresql.org/docs/current/protocol-flow.html
        // The actual PasswordMessage can be computed in SQL as concat('md5', md5(concat(md5(concat(password, username)), random-salt))).

        //SELECT concat('md5', md5(concat(md5(concat('postgres', 'postgres')), 'abcd')));
        const string expected = "md50800ba833a175e428fc0110c29998942";
        var actual = PasswordMessage.HashPassword("postgres", "postgres", Salt, PgOptions.PasswordEncoding);
        Assert.AreEqual(expected, actual);
    }
}

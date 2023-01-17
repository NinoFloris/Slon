using System;
using Slon.Protocol.PgV3;

namespace Slon.Pg;

public class PostgresException: Exception
{
    public PostgresException(string message) : base(message)
    { }

    internal PostgresException(ErrorOrNoticeMessage details)
        : base(details.Message)
    {
    }

    internal static void Throw(ErrorOrNoticeMessage details)
        => throw new PostgresException(details);
}

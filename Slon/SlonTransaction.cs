using System;
using System.Data;
using System.Data.Common;

namespace Slon;

public sealed class SlonTransaction : DbTransaction
{
    public override void Commit()
    {
        throw new NotImplementedException();
    }

    public override void Rollback()
    {
        throw new NotImplementedException();
    }

    protected override DbConnection? DbConnection { get; }
    public override IsolationLevel IsolationLevel { get; }
}

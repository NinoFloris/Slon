using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Npgsql.Pipelines.Protocol;

namespace Npgsql.Pipelines;

public class NpgsqlParameter: DbParameter
{
    public override void ResetDbType()
    {
        throw new System.NotImplementedException();
    }

    public override DbType DbType { get; set; }
    public override ParameterDirection Direction { get; set; }
    public override bool IsNullable { get; set; }
    [AllowNull]
    public override string ParameterName { get; set; }
    [AllowNull]
    public override string SourceColumn { get; set; }
    [AllowNull]
    public override object Value { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }

    internal FormatCode FormatCode { get; }
    internal Oid Oid { get; }
}

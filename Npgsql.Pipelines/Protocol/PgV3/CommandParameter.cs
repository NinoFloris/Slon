using Npgsql.Pipelines.Pg.Types;
using Npgsql.Pipelines.Protocol.PgV3.Descriptors;

namespace Npgsql.Pipelines.Protocol.PgV3;

sealed class PgV3ParameterInfo: ParameterInfo
{
    protected override bool GetIsBinary() => Parameter.Type != WellKnownTypes.Text;
    protected override int? GetLength() => Parameter.Type.Length;

    public Parameter Parameter { get; }
    public FormatCode FormatCode => IsBinary ? FormatCode.Binary : FormatCode.Text;
}

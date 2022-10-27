namespace Npgsql.Pipelines.Protocol.PgV3.Types;

/// A descriptive record on a single field being returned for a query.
/// See RowDescription in https://www.postgresql.org/docs/current/static/protocol-message-formats.html
readonly record struct Field(string Name, Oid TableOid, short ColumnAttributeNumber, Oid Oid, short TypeSize, int TypeModifier, FormatCode FormatCode)
{
    public bool IsBinaryFormat => FormatCode is FormatCode.Binary;
    public bool IsTextFormat => FormatCode is FormatCode.Text;
}

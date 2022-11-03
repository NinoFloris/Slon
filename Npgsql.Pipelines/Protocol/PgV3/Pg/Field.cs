namespace Npgsql.Pipelines.Protocol.PgV3.Types;

/// Base field type shared between tables and composites.
readonly record struct Field(string Name, Oid Oid, int TypeModifier, short TypeSize);

/// A descriptive record on a single field being returned for a query.
/// See RowDescription in https://www.postgresql.org/docs/current/static/protocol-message-formats.html
readonly record struct StatementField(Field Field, Oid TableOid, short ColumnAttributeNumber, FormatCode FormatCode)
{
    public bool IsBinaryFormat => FormatCode is FormatCode.Binary;
    public bool IsTextFormat => FormatCode is FormatCode.Text;
}

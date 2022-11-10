using Npgsql.Pipelines.Pg.Descriptors;
using Npgsql.Pipelines.Pg.Types;

namespace Npgsql.Pipelines.Protocol.PgV3.Descriptors;

/// A descriptive record on a single field being returned for a query.
/// See RowDescription in https://www.postgresql.org/docs/current/static/protocol-message-formats.html
readonly record struct StatementField(Field Field, short FieldTypeSize, int FieldTypeModifier, Oid TableOid, short ColumnAttributeNumber, FormatCode FormatCode)
{
    public bool IsBinaryFormat => FormatCode is FormatCode.Binary;
    public bool IsTextFormat => FormatCode is FormatCode.Text;
}

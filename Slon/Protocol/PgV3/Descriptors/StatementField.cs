using System;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Protocol.PgV3.Descriptors;

/// A descriptive record on a single field being returned for a query.
/// See RowDescription in https://www.postgresql.org/docs/current/static/protocol-message-formats.html
readonly struct StatementField
{
    public Field Field { get; }
    public short FieldTypeSize { get; }
    public Oid TableOid { get; }
    public short ColumnAttributeNumber { get; }
    public FormatCode FormatCode { get; }

    public StatementField(Field field, short fieldTypeSize, Oid tableOid, short columnAttributeNumber, FormatCode formatCode)
    {
        if (field.PgTypeId.IsDataTypeName)
            // Throw inlined as constructors will never be inlined.
            throw new ArgumentException("Instances of StatementField cannot have data type name field identifiers.", nameof(field));

        Field = field;
        FieldTypeSize = fieldTypeSize;
        TableOid = tableOid;
        ColumnAttributeNumber = columnAttributeNumber;
        FormatCode = formatCode;
    }

    public bool IsBinaryFormat => FormatCode is FormatCode.Binary;
    public bool IsTextFormat => FormatCode is FormatCode.Text;

    // Maximum amount of columns that will work on postgres.
    // See https://github.com/postgres/postgres/blob/c219d9b0a55bcdf81b00da6bad24ac2bf3e53d20/src/include/access/htup_details.h
    public const int MaxCount = 1600;
}

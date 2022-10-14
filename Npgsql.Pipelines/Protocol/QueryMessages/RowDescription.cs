using System;
using System.Buffers;
using System.Collections.Generic;

namespace Npgsql.Pipelines.Protocol;

class RowDescription: IBackendMessage
{
    static int ColumnCountLookupThreshold => 10;
    public const int MaxColumns = ushort.MaxValue;

    ArraySegment<FieldDescription> _fields;
    Dictionary<string, int>? _nameIndex;
    Dictionary<string, int>? _insensitiveIndex;

    public RowDescription(int fields = 10)
    {
        _fields = new ArraySegment<FieldDescription>(new FieldDescription[fields]);
    }

    public ArraySegment<FieldDescription> Fields => _fields;

    public ReadStatus Read(ref MessageReader reader)
    {
        if (!reader.MoveNextAndIsExpected(BackendCode.RowDescription, out var status, ensureBuffered: true))
            return status;

        // TODO read ushort.
        reader.TryReadShort(out var columnCount);
        if (_fields.Array!.Length >= columnCount)
            _fields = new ArraySegment<FieldDescription>(_fields.Array, 0, columnCount);
        else
            _fields = new ArraySegment<FieldDescription>(new FieldDescription[columnCount], 0, columnCount);
        var fields = _fields.Array!;
        Dictionary<string, int>? nameIndex = null;
        if (columnCount > ColumnCountLookupThreshold)
            nameIndex = new Dictionary<string, int>(columnCount, StringComparer.Ordinal);

        for (var i = 0; i < fields.Length && i < columnCount; i++)
        {
            reader.TryReadCString(out var name);
            reader.TryReadInt(out var tableOid);
            reader.TryReadShort(out var columnAttributeNumber);
            reader.TryReadInt(out var oid);
            reader.TryReadShort(out var typeSize);
            reader.TryReadInt(out var typeModifier);
            reader.TryReadShort(out var formatCode);
            fields[i] = new(
                name:                  name!,
                tableOid:              new Oid(tableOid),
                columnAttributeNumber: columnAttributeNumber,
                oid:                   new Oid(oid),
                typeSize:              typeSize,
                typeModifier:          typeModifier,
                formatCode:            (FormatCode)formatCode
            );
            if (nameIndex is not null)
                nameIndex.TryAdd(name!, i);
        }

        _nameIndex = nameIndex;

        reader.ConsumeCurrent();
        return ReadStatus.Done;
    }

    public void Reset()
    {
        _fields = new ArraySegment<FieldDescription>(_fields.Array, 0, 0);
    }
}


/// <summary>
/// A descriptive record on a single field received from PostgreSQL.
/// See RowDescription in https://www.postgresql.org/docs/current/static/protocol-message-formats.html
/// </summary>
public struct FieldDescription
{
    internal FieldDescription(
        string name, Oid tableOid, short columnAttributeNumber,
        Oid oid, short typeSize, int typeModifier, FormatCode formatCode)
    {
        Name = name;
        TableOid = tableOid;
        ColumnAttributeNumber = columnAttributeNumber;
        TypeOid = oid;
        TypeSize = typeSize;
        TypeModifier = typeModifier;
        FormatCode = formatCode;
    }

    /// <summary>
    /// The field name.
    /// </summary>
    internal string Name { get; set; }

    /// <summary>
    /// The object ID of the field's data type.
    /// </summary>
    internal Oid TypeOid { get; }

    /// <summary>
    /// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
    /// </summary>
    public short TypeSize { get; }

    /// <summary>
    /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
    /// </summary>
    public int TypeModifier { get; }

    /// <summary>
    /// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
    /// </summary>
    internal Oid TableOid { get; }

    /// <summary>
    /// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
    /// </summary>
    internal short ColumnAttributeNumber { get; }

    /// <summary>
    /// The format code being used for the field.
    /// Currently will be zero (text) or one (binary).
    /// In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
    /// </summary>
    internal FormatCode FormatCode { get; }

    // internal string TypeDisplayName => PostgresType.GetDisplayNameWithFacets(TypeModifier);

    // /// <summary>
    // /// The Npgsql type handler assigned to handle this field.
    // /// Returns <see cref="UnknownTypeHandler"/> for fields with format text.
    // /// </summary>
    // internal NpgsqlTypeHandler Handler { get; private set; }
    //
    // internal PostgresType PostgresType
    //     => _typeMapper.DatabaseInfo.ByOID.TryGetValue(TypeOID, out var postgresType)
    //         ? postgresType
    //         : UnknownBackendType.Instance;
    //
    // internal Type FieldType => Handler.GetFieldType(this);
    //
    // internal void ResolveHandler()
    //     => Handler = IsBinaryFormat ? _typeMapper.ResolveByOID(TypeOID) : _typeMapper.UnrecognizedTypeHandler;
    //
    // ConnectorTypeMapper _typeMapper;

    internal bool IsBinaryFormat => FormatCode == FormatCode.Binary;
    internal bool IsTextFormat => FormatCode == FormatCode.Text;

    internal FieldDescription Clone()
    {
        var field = new FieldDescription(Name, TableOid, ColumnAttributeNumber, TypeOid, TypeSize, TypeModifier, FormatCode);
        return field;
    }

    /// <summary>
    /// Returns a string that represents the current object.
    /// </summary>
    // public override string ToString() => Name + (Handler == null ? "" : $"({Handler.PgDisplayName})");
}

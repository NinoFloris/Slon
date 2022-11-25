using System;
using System.Collections.Generic;
using Npgsql.Pipelines.Pg.Descriptors;
using Npgsql.Pipelines.Pg.Types;
using Npgsql.Pipelines.Protocol.PgV3.Descriptors;

namespace Npgsql.Pipelines.Protocol.PgV3;

struct RowDescription: IPgV3BackendMessage
{
    public const int MaxColumns = ushort.MaxValue;
    static int ColumnCountLookupThreshold => 10;

    ArraySegment<StatementField> _fields;
    Dictionary<string, int>? _nameIndex;

    public RowDescription(int initialCapacity)
        => _fields = new ArraySegment<StatementField>(new StatementField[initialCapacity], 0, 0);

    public ReadOnlyMemory<StatementField> Fields => new(_fields.Array, _fields.Offset, _fields.Count);

    public ReadStatus Read(ref MessageReader<PgV3Header> reader)
    {
        if (!reader.MoveNextAndIsExpected(BackendCode.RowDescription, out var status, ensureBuffered: true))
            return status;

        reader.TryReadShort(out var columnCount);
        if (_fields.Array?.Length >= columnCount)
            _fields = new ArraySegment<StatementField>(_fields.Array, 0, columnCount);
        else
            _fields = new ArraySegment<StatementField>(new StatementField[columnCount], 0, columnCount);
        var fields = _fields.Array!;
        Dictionary<string, int>? nameIndex = null;
        if (columnCount > ColumnCountLookupThreshold)
            nameIndex ??= new Dictionary<string, int>(columnCount, StringComparer.Ordinal);

        for (var i = 0; i < fields.Length && i < columnCount; i++)
        {
            // TODO pool these chars, only converting them to strings when it'll be used for a prepared statement.
            reader.TryReadCString(out var name);
            reader.TryReadUInt(out var tableOid);
            reader.TryReadShort(out var columnAttributeNumber);
            reader.TryReadUInt(out var oid);
            reader.TryReadShort(out var typeSize);
            reader.TryReadInt(out var typeModifier);
            reader.TryReadShort(out var formatCode);
            fields[i] = new(
                new Field(
                    Name:              name!,
                    Oid:               new Oid(oid),
                    TypeModifier:      typeModifier
                ),
                FieldTypeSize:          typeSize,
                TableOid:              new Oid(tableOid),
                ColumnAttributeNumber: columnAttributeNumber,
                FormatCode:            (FormatCode)formatCode
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
        _fields = new ArraySegment<StatementField>(_fields.Array!, 0, 0);
        _nameIndex?.Clear();
    }
}

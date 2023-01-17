using System;
using System.Diagnostics.CodeAnalysis;

namespace Slon.Pg.Types;

readonly record struct PgTypeId
{
    readonly Oid _oid;
    readonly DataTypeName _dataTypeName;

    public PgTypeId(Oid oid) => _oid = oid;
    public PgTypeId(DataTypeName name) => _dataTypeName = name;

    public bool IsOid => _dataTypeName.IsDefault;
    [MemberNotNullWhen(true, nameof(_dataTypeName))]
    public bool IsDataTypeName => !_dataTypeName.IsDefault;

    public Oid Oid
    {
        get
        {
            if (!IsOid)
                throw new InvalidOperationException("This value does not describe an Oid.");

            return _oid;
        }
    }

    public DataTypeName DataTypeName
    {
        get
        {
            if (!IsDataTypeName)
                throw new InvalidOperationException("This value does not describe a DataTypeName.");

            return _dataTypeName;
        }
    }

    public static implicit operator PgTypeId(Oid id) => new(id);
    public static implicit operator PgTypeId(DataTypeName name) => new(name);

    public bool Equals(PgTypeId other)
        => IsOid ? _oid.Value == other._oid.Value : _dataTypeName.Value == other._dataTypeName.Value;

    public override int GetHashCode() => IsOid ? _oid.GetHashCode() : _dataTypeName.GetHashCode();

    public override string ToString() => IsOid ? _oid.ToString() : _dataTypeName.Value;
}

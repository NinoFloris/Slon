using System;

namespace Npgsql.Pipelines.Pg.Types;

// TODO composite should carry a PgType in every field, either Field needs to become a DU of PgTypeId | PgType
// TODO so it can be shared across RowDescription and this, or we need to stop sharing them.
readonly record struct PgType(PgTypeId Identifier, PgKind Kind)
{
    /// Recursively checks whether all type ids part of this type are portable across backends.
    public bool IsPortable =>
        Identifier.IsDataTypeName && Kind switch
        {
            PgKind.Base => true,
            PgKind.Array array => array.ElementType.IsPortable,
            PgKind.Range range => range.ElementType.IsPortable,
            PgKind.MultiRange multiRange => multiRange.RangeType.IsPortable,
            PgKind.Enum => true,
            PgKind.Pseudo => true,
            PgKind.Composite composite => IsCompositePortable(composite),
            PgKind.Domain domain => domain.UnderlyingType.IsPortable,
            var v => throw new ArgumentOutOfRangeException(nameof(Kind), v, null)
        };

    /// Returns the element type for arrays, ranges and multi ranges, composites return null as they don't have a singular base type, returns itself for all the other cases.
    public PgType? BaseType =>
        Kind switch
        {
            PgKind.Base => this,
            PgKind.Domain => this,
            PgKind.Pseudo => this,
            PgKind.Enum => this,
            PgKind.Array array => array.ElementType,
            PgKind.Range range => range.ElementType,
            PgKind.MultiRange multiRange => multiRange.RangeType.BaseType,
            PgKind.Composite => null,
            var v => throw new ArgumentOutOfRangeException(nameof(Kind), v, null)
        };

    bool IsCompositePortable(PgKind.Composite composite)
    {
        foreach (var field in composite.Fields)
            if (!field.PgTypeId.IsDataTypeName)
                return false;

        return true;
    }
}

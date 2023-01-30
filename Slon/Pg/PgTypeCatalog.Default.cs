using System;
using System.Collections.Generic;
using Slon.Pg.Types;
using static Slon.Pg.PgTypeGroup;

namespace Slon.Pg;

partial class PgTypeCatalog
{
    // A default set of non-portable postgres types which are commonly supported, to be used in offline scenarios (no type loading) or testing.
    static PgTypeCatalog? _defaultPgTypeCatalog;
    public static PgTypeCatalog Default
    {
        get
        {
            return _defaultPgTypeCatalog ??= new PgTypeCatalog(GeneratePairs());

            IEnumerable<KeyValuePair<DataTypeName,PgType>> GeneratePairs()
            {
                foreach (var info in DefaultPgTypes.Items)
                {
                    yield return new(info.BaseName, info.BaseType);
                    yield return new(info.ArrayName, info.ArrayType);
                    if (info.RangeName is { } rangeName)
                        yield return new(rangeName, info.RangeType!.Value);
                    if (info.MultiRangeName is { } multiRangeName)
                        yield return new(multiRangeName, info.MultiRangeType!.Value);
                }
            }
        }
    }
}

static class DefaultPgTypes
{
    // We could also codegen this from pg_type.dat that lives in the postgres repo.
    public static IEnumerable<PgTypeGroup> Items
    {
        get
        {
            yield return Create(DataTypeNames.Int2, baseOid: 21, arrayOid: 1005);
            yield return Create(DataTypeNames.Int4, baseOid: 23, arrayOid: 1007, rangeName: "int4range", rangeOid: 3904, multiRangeName: "int4multirange", multiRangeOid: 4451);
            yield return Create(DataTypeNames.Int8, baseOid: 20, arrayOid: 1016, rangeName: "int8range", rangeOid: 3926, multiRangeName: "int8multirange", multiRangeOid: 4536);
            yield return Create(DataTypeNames.Float4, baseOid: 700, arrayOid: 1021);
            yield return Create(DataTypeNames.Float8, baseOid: 701, arrayOid: 1022);
            yield return Create(DataTypeNames.Numeric, baseOid: 1700, arrayOid: 1231, rangeName: "numrange", rangeOid: 3906, multiRangeName: "nummultirange", multiRangeOid: 4532);
            yield return Create(DataTypeNames.Money, baseOid: 790, arrayOid: 791);

            yield return Create(DataTypeNames.Bool, baseOid: 16, arrayOid: 1000);

            yield return Create(DataTypeNames.Box, baseOid: 603, arrayOid: 1020);
            yield return Create(DataTypeNames.Circle, baseOid: 718, arrayOid: 719);
            yield return Create(DataTypeNames.Line, baseOid: 628, arrayOid: 629);
            yield return Create(DataTypeNames.Lseg, baseOid: 601, arrayOid: 1018);
            yield return Create(DataTypeNames.Path, baseOid: 602, arrayOid: 1019);
            yield return Create(DataTypeNames.Point, baseOid: 600, arrayOid: 1017);
            yield return Create(DataTypeNames.Polygon, baseOid: 604, arrayOid: 1027);

            yield return Create(DataTypeNames.Bpchar, baseOid: 1042, arrayOid: 1014);
            yield return Create(DataTypeNames.Text, baseOid: 25, arrayOid: 1009);
            yield return Create(DataTypeNames.Varchar, baseOid: 1043, arrayOid: 1015);
            yield return Create(DataTypeNames.Name, baseOid: 19, arrayOid: 1003);

            yield return Create(DataTypeNames.Bytea, baseOid: 17, arrayOid: 1001);

            yield return Create(DataTypeNames.Date, baseOid: 1082, arrayOid: 1182, rangeName: "daterange", rangeOid: 3912, multiRangeName: "datemultirange", multiRangeOid: 4535);
            yield return Create(DataTypeNames.Time, baseOid: 1083, arrayOid: 1183);
            yield return Create(DataTypeNames.Timestamp, baseOid: 1114, arrayOid: 1115, rangeName: "tsrange", rangeOid: 3908, multiRangeName: "tsmultirange", multiRangeOid: 4533);
            yield return Create(DataTypeNames.TimestampTz, baseOid: 1184, arrayOid: 1185, rangeName: "tstzrange", rangeOid: 3910, multiRangeName: "tstzmultirange", multiRangeOid: 4534);
            yield return Create(DataTypeNames.Interval, baseOid: 1186, arrayOid: 1187);
            yield return Create(DataTypeNames.TimeTz, baseOid: 1266, arrayOid: 1270);

            yield return Create(DataTypeNames.Inet, baseOid: 869, arrayOid: 1041);
            yield return Create(DataTypeNames.Cidr, baseOid: 650, arrayOid: 651);
            yield return Create(DataTypeNames.MacAddr, baseOid: 829, arrayOid: 1040);
            yield return Create(DataTypeNames.MacAddr8, baseOid: 774, arrayOid: 775);

            yield return Create(DataTypeNames.Bit, baseOid: 1560, arrayOid: 1561);
            yield return Create(DataTypeNames.Varbit, baseOid: 1562, arrayOid: 1563);

            yield return Create(DataTypeNames.TsVector, baseOid: 3614, arrayOid: 3643);
            yield return Create(DataTypeNames.TsQuery, baseOid: 3615, arrayOid: 3645);
            yield return Create(DataTypeNames.RegConfig, baseOid: 3734, arrayOid: 3735);

            yield return Create(DataTypeNames.Uuid, baseOid: 2950, arrayOid: 2951);
            yield return Create(DataTypeNames.Xml, baseOid: 142, arrayOid: 143);
            yield return Create(DataTypeNames.Json, baseOid: 114, arrayOid: 199);
            yield return Create(DataTypeNames.Jsonb, baseOid: 3802, arrayOid: 3807);
            yield return Create(DataTypeNames.JsonPath, baseOid: 4072, arrayOid: 4073);

            yield return Create(DataTypeNames.RefCursor, baseOid: 1790, arrayOid: 2201);
            yield return Create(DataTypeNames.OidVector, baseOid: 30, arrayOid: 1013);
            yield return Create(DataTypeNames.Int2Vector, baseOid: 22, arrayOid: 1006);
            yield return Create(DataTypeNames.Oid, baseOid: 26, arrayOid: 1028);
            yield return Create(DataTypeNames.Xid, baseOid: 28, arrayOid: 1011);
            yield return Create(DataTypeNames.Xid8, baseOid: 5069, arrayOid: 271);
            yield return Create(DataTypeNames.Cid, baseOid: 29, arrayOid: 1012);
            yield return Create(DataTypeNames.RegType, baseOid: 2206, arrayOid: 2211);
            yield return Create(DataTypeNames.Tid, baseOid: 27, arrayOid: 1010);
            yield return Create(DataTypeNames.PgLsn, baseOid: 3220, arrayOid: 3221);

            yield return Create(DataTypeNames.Unknown, baseOid: 705, arrayOid: 0, baseTypeKind: PgKind.PseudoKind);
        }
    }
}

readonly struct PgTypeGroup
{
    public required DataTypeName BaseName { get; init; }
    public required PgType BaseType { get; init; }
    public required DataTypeName ArrayName { get; init; }
    public required PgType ArrayType { get; init; }
    public DataTypeName? RangeName { get; init; }
    public PgType? RangeType { get; init; }
    public DataTypeName? MultiRangeName { get; init; }
    public PgType? MultiRangeType { get; init; }

    public static PgTypeGroup Create(DataTypeName baseName, Oid baseOid, Oid arrayOid, string? rangeName = null, Oid? rangeOid = null, string? multiRangeName = null, Oid? multiRangeOid = null, PgKind? baseTypeKind = null)
    {
        if (rangeName is not null && rangeOid is null || rangeName is null && rangeOid is not null)
            throw new ArgumentException("Range oid and name have to both be supplied, one or the other cannot be omitted.");

        if (multiRangeName is not null && multiRangeOid is null || multiRangeName is null && multiRangeOid is not null)
            throw new ArgumentException("MultiRange oid and name have to both be supplied, one or the other cannot be omitted.");

        if (rangeName is null && multiRangeName is not null)
            throw new ArgumentException("When a multirange is supplied its range cannot be omitted.");

        var baseType = new PgType(baseOid, baseTypeKind ?? PgKind.BaseKind);
        PgType? rangeType;
        return new PgTypeGroup
        {
            BaseName = baseName,
            BaseType = baseType,

            ArrayName = DataTypeName.CreateFullyQualifiedName("_" + baseName.Value.Substring(baseName.Value.IndexOf('.'))),
            ArrayType = new(arrayOid, new PgKind.Array(baseType)),

            RangeName = rangeName is { } ? DataTypeName.CreateFullyQualifiedName(rangeName) : null,
            RangeType = rangeType = rangeOid is { } rangeOidValue ? new(rangeOidValue, new PgKind.Range(baseType)) : null,

            MultiRangeName = multiRangeName is { } ? DataTypeName.CreateFullyQualifiedName(multiRangeName) : null,
            MultiRangeType = multiRangeOid is { } multiRangeOidValue ? new(multiRangeOidValue, new PgKind.MultiRange(rangeType.GetValueOrDefault())) : null,
        };
    }
}

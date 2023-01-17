using System;
using System.Data;
using Slon.Pg.Types;

namespace Slon;

// TODO add friendly aliases (short etc)
public static class SlonDbTypes
{
    public static SlonDbType Int2 => new(DataTypeNames.Int2);
    public static SlonDbType Int4 => new(DataTypeNames.Int4);
    public static SlonDbType Int8 => new(DataTypeNames.Int8);
    public static SlonDbType Float4 => new(DataTypeNames.Float4);
    public static SlonDbType Float8 => new(DataTypeNames.Float8);
    public static SlonDbType Numeric => new(DataTypeNames.Numeric);
    public static SlonDbType Money => new(DataTypeNames.Money);
    public static SlonDbType Bool => new(DataTypeNames.Bool);
    public static SlonDbType Box => new(DataTypeNames.Box);
    public static SlonDbType Circle => new(DataTypeNames.Circle);
    public static SlonDbType Line => new(DataTypeNames.Line);
    public static SlonDbType Lseg => new(DataTypeNames.Lseg);
    public static SlonDbType Path => new(DataTypeNames.Path);
    public static SlonDbType Point => new(DataTypeNames.Point);
    public static SlonDbType Polygon => new(DataTypeNames.Polygon);
    public static SlonDbType Bpchar => new(DataTypeNames.Bpchar);
    public static SlonDbType Text => new(DataTypeNames.Text);
    public static SlonDbType Varchar => new(DataTypeNames.Varchar);
    public static SlonDbType Name => new(DataTypeNames.Name);
    public static SlonDbType Bytea => new(DataTypeNames.Bytea);
    public static SlonDbType Date => new(DataTypeNames.Date);
    public static SlonDbType Time => new(DataTypeNames.Time);
    public static SlonDbType Timestamp => new(DataTypeNames.Timestamp);
    public static SlonDbType TimestampTz => new(DataTypeNames.TimestampTz);
    public static SlonDbType Interval => new(DataTypeNames.Interval);
    public static SlonDbType TimeTz => new(DataTypeNames.TimeTz);
    public static SlonDbType Inet => new(DataTypeNames.Inet);
    public static SlonDbType Cidr => new(DataTypeNames.Cidr);
    public static SlonDbType MacAddr => new(DataTypeNames.MacAddr);
    public static SlonDbType MacAddr8 => new(DataTypeNames.MacAddr8);
    public static SlonDbType Bit => new(DataTypeNames.Bit);
    public static SlonDbType Varbit => new(DataTypeNames.Varbit);
    public static SlonDbType TsVector => new(DataTypeNames.TsVector);
    public static SlonDbType TsQuery => new(DataTypeNames.TsQuery);
    public static SlonDbType RegConfig => new(DataTypeNames.RegConfig);
    public static SlonDbType Uuid => new(DataTypeNames.Uuid);
    public static SlonDbType Xml => new(DataTypeNames.Xml);
    public static SlonDbType Json => new(DataTypeNames.Json);
    public static SlonDbType Jsonb => new(DataTypeNames.Jsonb);
    public static SlonDbType JsonPath => new(DataTypeNames.JsonPath);
    public static SlonDbType RefCursor => new(DataTypeNames.RefCursor);
    public static SlonDbType OidVector => new(DataTypeNames.OidVector);
    public static SlonDbType Int2Vector => new(DataTypeNames.Int2Vector);
    public static SlonDbType Oid => new(DataTypeNames.Oid);
    public static SlonDbType Xid => new(DataTypeNames.Xid);
    public static SlonDbType Xid8 => new(DataTypeNames.Xid8);
    public static SlonDbType Cid => new(DataTypeNames.Cid);
    public static SlonDbType RegType => new(DataTypeNames.RegType);
    public static SlonDbType Tid => new(DataTypeNames.Tid);
    public static SlonDbType PgLsn => new(DataTypeNames.PgLsn);
    public static SlonDbType Unknown => new(DataTypeNames.Unknown);

    internal static SlonDbType ToSlonDbType(DbType dbType)
        => dbType switch
        {
            DbType.AnsiString            => Text,
            DbType.Binary                => Bytea,
            DbType.Byte                  => Int2,
            DbType.SByte                 => Int2,
            DbType.Boolean               => Bool,
            DbType.Currency              => Money,
            DbType.Date                  => Date,
            DbType.DateTime              => TimestampTz,
            DbType.Decimal               => Numeric,
            DbType.VarNumeric            => Numeric,
            DbType.Double                => Float8,
            DbType.Guid                  => Uuid,
            DbType.Int16                 => Int2,
            DbType.Int32                 => Int4,
            DbType.Int64                 => Int8,
            DbType.Single                => Float4,
            DbType.String                => Text,
            DbType.Time                  => Time,
            DbType.AnsiStringFixedLength => Text,
            DbType.StringFixedLength     => Text,
            DbType.Xml                   => Xml,
            DbType.DateTime2             => Timestamp,
            DbType.DateTimeOffset        => TimestampTz,

            DbType.Object                => SlonDbType.Infer,
            DbType.UInt16                => SlonDbType.Infer,
            DbType.UInt32                => SlonDbType.Infer,
            DbType.UInt64                => SlonDbType.Infer,

            _ => throw new ArgumentOutOfRangeException(nameof(dbType), dbType, null)
        };
}

// A potentially invalid or unknown type identifier, used in frontend operations like configuring DbParameter types.
// The DbDataSource this is passed to decides on the validity of the contents.
public readonly record struct SlonDbType
{
    readonly string? _dataTypeName;

    internal SlonDbType(DataTypeName dataTypeName)
        : this((string)dataTypeName)
    {}

    internal SlonDbType(string dataTypeName)
    {
        _dataTypeName = dataTypeName;
    }

    internal bool IsInfer => _dataTypeName is null;
    internal string DataTypeName => _dataTypeName ?? throw new InvalidOperationException("SlonDbType does not carry a name.");

    internal bool ResolveArrayType { get; init; }
    internal bool ResolveMultiRangeType { get; init; }

    public SlonDbType AsArray() => this with { ResolveArrayType = true };
    public SlonDbType AsMultiRange() => this with { ResolveMultiRangeType = true };

    public override string ToString() => IsInfer
        ? @"Case = ""Inference"""
        : $@"Case = ""DataTypeName"", Value = ""{Pg.Types.DataTypeName.CreateFullyQualifiedName(DataTypeName).DisplayName}{(ResolveArrayType ? "[]" : "")}""";

    /// Infer a database type from the parameter value instead of specifying one.
    public static SlonDbType Infer => default;
    public static SlonDbType Create(string dataTypeName) => new(dataTypeName.Trim());

    // public DbType? ToDbType() => SlonDbTypes.ToDbType(this);
    public static explicit operator SlonDbType(DbType dbType) => SlonDbTypes.ToSlonDbType(dbType);
}

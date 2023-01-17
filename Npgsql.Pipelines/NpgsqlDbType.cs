using System;
using System.Data;
using Npgsql.Pipelines.Pg.Types;

namespace Npgsql.Pipelines;

// TODO add friendly aliases (short etc)
public static class NpgsqlDbTypes
{
    public static NpgsqlDbType Int2 => new(DataTypeNames.Int2);
    public static NpgsqlDbType Int4 => new(DataTypeNames.Int4);
    public static NpgsqlDbType Int8 => new(DataTypeNames.Int8);
    public static NpgsqlDbType Float4 => new(DataTypeNames.Float4);
    public static NpgsqlDbType Float8 => new(DataTypeNames.Float8);
    public static NpgsqlDbType Numeric => new(DataTypeNames.Numeric);
    public static NpgsqlDbType Money => new(DataTypeNames.Money);
    public static NpgsqlDbType Bool => new(DataTypeNames.Bool);
    public static NpgsqlDbType Box => new(DataTypeNames.Box);
    public static NpgsqlDbType Circle => new(DataTypeNames.Circle);
    public static NpgsqlDbType Line => new(DataTypeNames.Line);
    public static NpgsqlDbType Lseg => new(DataTypeNames.Lseg);
    public static NpgsqlDbType Path => new(DataTypeNames.Path);
    public static NpgsqlDbType Point => new(DataTypeNames.Point);
    public static NpgsqlDbType Polygon => new(DataTypeNames.Polygon);
    public static NpgsqlDbType Bpchar => new(DataTypeNames.Bpchar);
    public static NpgsqlDbType Text => new(DataTypeNames.Text);
    public static NpgsqlDbType Varchar => new(DataTypeNames.Varchar);
    public static NpgsqlDbType Name => new(DataTypeNames.Name);
    public static NpgsqlDbType Bytea => new(DataTypeNames.Bytea);
    public static NpgsqlDbType Date => new(DataTypeNames.Date);
    public static NpgsqlDbType Time => new(DataTypeNames.Time);
    public static NpgsqlDbType Timestamp => new(DataTypeNames.Timestamp);
    public static NpgsqlDbType TimestampTz => new(DataTypeNames.TimestampTz);
    public static NpgsqlDbType Interval => new(DataTypeNames.Interval);
    public static NpgsqlDbType TimeTz => new(DataTypeNames.TimeTz);
    public static NpgsqlDbType Inet => new(DataTypeNames.Inet);
    public static NpgsqlDbType Cidr => new(DataTypeNames.Cidr);
    public static NpgsqlDbType MacAddr => new(DataTypeNames.MacAddr);
    public static NpgsqlDbType MacAddr8 => new(DataTypeNames.MacAddr8);
    public static NpgsqlDbType Bit => new(DataTypeNames.Bit);
    public static NpgsqlDbType Varbit => new(DataTypeNames.Varbit);
    public static NpgsqlDbType TsVector => new(DataTypeNames.TsVector);
    public static NpgsqlDbType TsQuery => new(DataTypeNames.TsQuery);
    public static NpgsqlDbType RegConfig => new(DataTypeNames.RegConfig);
    public static NpgsqlDbType Uuid => new(DataTypeNames.Uuid);
    public static NpgsqlDbType Xml => new(DataTypeNames.Xml);
    public static NpgsqlDbType Json => new(DataTypeNames.Json);
    public static NpgsqlDbType Jsonb => new(DataTypeNames.Jsonb);
    public static NpgsqlDbType JsonPath => new(DataTypeNames.JsonPath);
    public static NpgsqlDbType RefCursor => new(DataTypeNames.RefCursor);
    public static NpgsqlDbType OidVector => new(DataTypeNames.OidVector);
    public static NpgsqlDbType Int2Vector => new(DataTypeNames.Int2Vector);
    public static NpgsqlDbType Oid => new(DataTypeNames.Oid);
    public static NpgsqlDbType Xid => new(DataTypeNames.Xid);
    public static NpgsqlDbType Xid8 => new(DataTypeNames.Xid8);
    public static NpgsqlDbType Cid => new(DataTypeNames.Cid);
    public static NpgsqlDbType RegType => new(DataTypeNames.RegType);
    public static NpgsqlDbType Tid => new(DataTypeNames.Tid);
    public static NpgsqlDbType PgLsn => new(DataTypeNames.PgLsn);
    public static NpgsqlDbType Unknown => new(DataTypeNames.Unknown);

    internal static NpgsqlDbType ToNpgsqlDbType(DbType dbType)
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

            DbType.Object                => NpgsqlDbType.Infer,
            DbType.UInt16                => NpgsqlDbType.Infer,
            DbType.UInt32                => NpgsqlDbType.Infer,
            DbType.UInt64                => NpgsqlDbType.Infer,

            _ => throw new ArgumentOutOfRangeException(nameof(dbType), dbType, null)
        };
}

// A potentially invalid or unknown type identifier, used in frontend operations like configuring DbParameter types.
// The DbDataSource this is passed to decides on the validity of the contents.
public readonly record struct NpgsqlDbType
{
    readonly string? _dataTypeName;

    internal NpgsqlDbType(DataTypeName dataTypeName)
        : this((string)dataTypeName)
    {}

    internal NpgsqlDbType(string dataTypeName)
    {
        _dataTypeName = dataTypeName;
    }

    internal bool IsInfer => _dataTypeName is null;
    internal string DataTypeName => _dataTypeName ?? throw new InvalidOperationException("NpgsqlDbType does not carry a name.");

    internal bool ResolveArrayType { get; init; }
    internal bool ResolveMultiRangeType { get; init; }

    public NpgsqlDbType AsArray() => this with { ResolveArrayType = true };
    public NpgsqlDbType AsMultiRange() => this with { ResolveMultiRangeType = true };

    public override string ToString() => IsInfer
        ? @"Case = ""Inference"""
        : $@"Case = ""DataTypeName"", Value = ""{Pg.Types.DataTypeName.CreateFullyQualifiedName(DataTypeName).DisplayName}{(ResolveArrayType ? "[]" : "")}""";

    /// Infer a database type from the parameter value instead of specifying one.
    public static NpgsqlDbType Infer => default;
    public static NpgsqlDbType Create(string dataTypeName) => new(dataTypeName.Trim());

    // public DbType? ToDbType() => NpgsqlDbTypes.ToDbType(this);
    public static explicit operator NpgsqlDbType(DbType dbType) => NpgsqlDbTypes.ToNpgsqlDbType(dbType);
}

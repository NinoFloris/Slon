using System.Collections.Immutable;

namespace Npgsql.Pipelines.Protocol.PgV3.Types;

/// Enum of the kind of types supported by postgres.
abstract record PgKind(PgKind.Cases Tag)
{
    public enum Cases
    {
        /// A built-in type.
        Simple,
        /// An enum carying its variants.
        Enum,
        /// A pseudo type like anyarray.
        Pseudo,
        // An array carying its element type.
        Array,
        // A range carying its element type.
        Range,
        // A multi-range carying its element type.
        MultiRange,
        // A domain carying its underlying type.
        Domain,
        // A composite carying its constituent fields.
        Composite
    }

    public sealed record Simple() : PgKind(Cases.Simple)
    {
        public static Simple Instance => new();
    }

    public sealed record Enum(ImmutableArray<string> Variants) : PgKind(Cases.Enum);
    public sealed record Pseudo() : PgKind(Cases.Pseudo)
    {
        public static Pseudo Instance => new();
    }

    public sealed record Array(PgType ElementType) : PgKind(Cases.Array);
    public sealed record Range(PgType ElementType) : PgKind(Cases.Range);
    public sealed record MultiRange(PgType ElementType) : PgKind(Cases.MultiRange);
    public sealed record Domain(PgType UnderlyingType) : PgKind(Cases.Domain);

    // TODO this should host field info
    public sealed record Composite(ImmutableArray<PgType> Fields) : PgKind(Cases.Composite);
}

readonly record struct PgType(Oid Oid, string Schema, PgKind Kind)
{
    const string TypeCatalogSchema = "pg_catalog";
    public static PgType Unknown = new(Oid.Unknown, TypeCatalogSchema, PgKind.Pseudo.Instance);
}


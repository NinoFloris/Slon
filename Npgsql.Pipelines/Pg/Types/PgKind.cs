using Npgsql.Pipelines.Pg.Descriptors;

namespace Npgsql.Pipelines.Pg.Types;

/// Enum of the kind of types supported by postgres.
abstract record PgKind
{
    private protected PgKind(Cases Tag) {}

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

    public sealed record Simple : PgKind
    {
        Simple() : base(Cases.Simple) {}
        public static Simple Instance => new();
    }

    public sealed record Enum(StructuralArray<string> Variants) : PgKind(Cases.Enum);
    public sealed record Pseudo : PgKind
    {
        Pseudo() : base(Cases.Pseudo) {}
        public static Pseudo Instance => new();
    }

    public sealed record Array(PgType ElementType) : PgKind(Cases.Array);
    public sealed record Range(PgType ElementType) : PgKind(Cases.Range);
    public sealed record MultiRange(PgType ElementType) : PgKind(Cases.MultiRange);
    public sealed record Domain(PgType UnderlyingType) : PgKind(Cases.Domain);
    public sealed record Composite(StructuralArray<Field> Fields) : PgKind(Cases.Composite);

    public static Simple SimpleKind => Simple.Instance;
    public static Pseudo PseudoKind => Pseudo.Instance;
}

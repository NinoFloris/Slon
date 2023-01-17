using Npgsql.Pipelines.Pg.Descriptors;

namespace Npgsql.Pipelines.Pg.Types;

// TODO we want the case constructors to validate whether their passed in type kinds are what they expect (elementtype can't be another array etc).

/// Enum of the kind of types supported by postgres.
abstract record PgKind
{
    PgKind(Case tag) => Tag = tag;
    public Case Tag { get; }

    public enum Case
    {
        /// A base type.
        Base,
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

    public sealed record Base : PgKind
    {
        Base() : base(Case.Base) {}
        public static Base Instance => new();
    }

    public sealed record Enum(StructuralArray<string> Variants) : PgKind(Case.Enum);
    public sealed record Pseudo : PgKind
    {
        Pseudo() : base(Case.Pseudo) {}
        public static Pseudo Instance => new();
    }

    public sealed record Array(PgType ElementType) : PgKind(Case.Array);
    public sealed record Range(PgType ElementType) : PgKind(Case.Range);
    public sealed record MultiRange(PgType RangeType) : PgKind(Case.MultiRange);
    public sealed record Domain(PgType UnderlyingType) : PgKind(Case.Domain);
    public sealed record Composite(StructuralArray<Field> Fields) : PgKind(Case.Composite);

    public static Base BaseKind => Base.Instance;
    public static Pseudo PseudoKind => Pseudo.Instance;
}

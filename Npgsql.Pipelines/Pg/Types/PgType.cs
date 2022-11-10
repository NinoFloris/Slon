namespace Npgsql.Pipelines.Pg.Types;

readonly record struct PgType(Oid Oid, PgType.TypeLength Length, PgKind Kind, string Schema = PgType.TypeCatalogSchema)
{
    const string TypeCatalogSchema = "pg_catalog";
    public static PgType Unknown => new(Oid.Unknown, VariableLength, PgKind.PseudoKind);
    public static TypeLength VariableLength => new(-1);

    public readonly record struct TypeLength
    {
        readonly short _value;

        public TypeLength(short value) => _value = value;

        short? Value => IsVariable ? null : _value;
        bool IsVariable => _value == -1;

        public static implicit operator TypeLength(short value) => new(value);
        public static implicit operator short?(TypeLength instance) => instance.Value;
    }
}

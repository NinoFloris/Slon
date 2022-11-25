namespace Npgsql.Pipelines.Pg.Types;

// TODO probably want to codegen this... see pg_type.dat in the pg project. we'd only want to produce backing fields for constructed kinds.
static class WellKnownTypes
{
    public static PgType Bool => new(16, sizeof(bool), PgKind.SimpleKind);
    public static PgType Int4 => new(23, sizeof(int), PgKind.SimpleKind);
    public static PgType Text => new(25, PgType.VariableLength, PgKind.SimpleKind);

}

using Npgsql.Pipelines.Pg.Types;

namespace Npgsql.Pipelines.Pg.Descriptors;

/// Base field type shared between tables and composites.
readonly record struct Field(string Name, Oid Oid)
{
    public Field(string Name, PgType type) : this(Name, type.Oid) { }
}

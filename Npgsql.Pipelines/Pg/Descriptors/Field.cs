using Npgsql.Pipelines.Pg.Types;

namespace Npgsql.Pipelines.Pg.Descriptors;

/// Base field type shared between tables and composites.
readonly record struct Field(string Name, Oid Oid, int TypeModifier)
{
    public Field(string Name, PgType Type, int TypeModifier) : this(Name, Type.Oid, TypeModifier) { }
}

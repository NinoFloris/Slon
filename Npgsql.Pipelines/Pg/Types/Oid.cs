namespace Npgsql.Pipelines.Pg.Types;

readonly record struct Oid(uint Value)
{
    public static Oid Unknown => new(0);
    public static explicit operator uint(Oid oid) => oid.Value;
    public static implicit operator Oid(uint oid) => new(oid);
}

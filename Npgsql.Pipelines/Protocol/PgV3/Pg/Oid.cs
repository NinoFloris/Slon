namespace Npgsql.Pipelines.Protocol.PgV3.Types;

readonly record struct Oid(int Value)
{
    public static Oid Unknown => new(0);
    public static implicit operator int(Oid oid) => oid.Value;
}

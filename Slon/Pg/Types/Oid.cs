namespace Slon.Pg.Types;

readonly record struct Oid(uint Value)
{
    public static explicit operator uint(Oid oid) => oid.Value;
    public static implicit operator Oid(uint oid) => new(oid);
}

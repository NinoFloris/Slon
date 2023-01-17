namespace Slon.Pg.Types;

readonly struct PgTypeIdentifiers
{
    public PgTypeIdentifiers(Oid oid, DataTypeName dataTypeName)
    {
        Oid = oid;
        DataTypeName = dataTypeName;
    }

    public Oid Oid { get; }
    public DataTypeName DataTypeName { get; }
}

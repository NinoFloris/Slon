using System.Runtime.CompilerServices;

namespace Slon.Pg.Converters;

sealed class BoolConverter : PgBufferedConverter<bool>
{
    protected override bool ReadCore(PgReader reader) => reader.ReadByte() != 0;
    public override ValueSize GetSize(ref SizeContext context, bool value) => sizeof(byte);
    public override void Write(PgWriter writer, bool value) => writer.WriteByte(Unsafe.As<bool, byte>(ref value));
}

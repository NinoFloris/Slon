using System.Runtime.CompilerServices;

namespace Slon.Pg.Converters;

sealed class BoolConverter : PgFixedBinarySizeConverter<bool>
{
    protected override byte BinarySize => sizeof(byte);
    protected override bool ReadCore(PgReader reader) => reader.ReadByte() != 0;
    public override void Write(PgWriter writer, bool value) => writer.WriteByte(Unsafe.As<bool, byte>(ref value));
}

sealed class ByteConverter : PgFixedBinarySizeConverter<byte>
{
    protected override byte BinarySize => sizeof(byte);
    protected override byte ReadCore(PgReader reader) => reader.ReadByte();
    public override void Write(PgWriter writer, byte value) => writer.WriteByte(value);
}

sealed class SByteConverter : PgFixedBinarySizeConverter<sbyte>
{
    protected override byte BinarySize => sizeof(sbyte);
    protected override sbyte ReadCore(PgReader reader) => (sbyte)reader.ReadByte();
    public override void Write(PgWriter writer, sbyte value) => writer.WriteByte((byte)value);
}

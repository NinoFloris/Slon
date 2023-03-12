using System.Runtime.CompilerServices;

namespace Slon.Pg.Converters;

sealed class BoolConverter : FixedBinarySizePgConverter<bool>
{
    protected override byte BinarySize => sizeof(byte);
    public override bool Read(PgReader reader, PgConverterOptions options) => reader.ReadByte() != 0;
    public override void Write(PgWriter writer, bool value, PgConverterOptions options) => writer.WriteByte(Unsafe.As<bool, byte>(ref value));
}

sealed class ByteConverter : FixedBinarySizePgConverter<byte>
{
    protected override byte BinarySize => sizeof(byte);
    public override byte Read(PgReader reader, PgConverterOptions options) => reader.ReadByte();
    public override void Write(PgWriter writer, byte value, PgConverterOptions options) => writer.WriteByte(value);
}

sealed class SByteConverter : FixedBinarySizePgConverter<sbyte>
{
    protected override byte BinarySize => sizeof(sbyte);
    public override sbyte Read(PgReader reader, PgConverterOptions options) => (sbyte)reader.ReadByte();
    public override void Write(PgWriter writer, sbyte value, PgConverterOptions options) => writer.WriteByte((byte)value);
}

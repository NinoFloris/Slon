using System.Runtime.CompilerServices;

namespace Slon.Pg.Converters;

sealed class BoolConverter : PgFixedBinarySizeConverter<bool>
{
    protected override byte BinarySize => sizeof(byte);
    protected override bool ReadCore(PgReader reader, PgConverterOptions options) => reader.ReadByte() != 0;
    public override void Write(PgWriter writer, bool value, PgConverterOptions options) => writer.WriteByte(Unsafe.As<bool, byte>(ref value));
}

sealed class ByteConverter : PgFixedBinarySizeConverter<byte>
{
    protected override byte BinarySize => sizeof(byte);
    protected override byte ReadCore(PgReader reader, PgConverterOptions options) => reader.ReadByte();
    public override void Write(PgWriter writer, byte value, PgConverterOptions options) => writer.WriteByte(value);
}

sealed class SByteConverter : PgFixedBinarySizeConverter<sbyte>
{
    protected override byte BinarySize => sizeof(sbyte);
    protected override sbyte ReadCore(PgReader reader, PgConverterOptions options) => (sbyte)reader.ReadByte();
    public override void Write(PgWriter writer, sbyte value, PgConverterOptions options) => writer.WriteByte((byte)value);
}

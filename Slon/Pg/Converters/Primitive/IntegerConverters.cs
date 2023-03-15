namespace Slon.Pg.Converters;

sealed class Int16Converter : FixedBinarySizePgConverter<short>
{
    protected override byte BinarySize => sizeof(short);
    public override short Read(PgReader reader, PgConverterOptions options) => reader.ReadInt16();
    public override void Write(PgWriter writer, short value, PgConverterOptions options) => writer.WriteInt16(value);
}

sealed class Int32Converter : FixedBinarySizePgConverter<int>
{
    protected override byte BinarySize => sizeof(int);
    public override int Read(PgReader reader, PgConverterOptions options) => reader.ReadInt32();
    public override void Write(PgWriter writer, int value, PgConverterOptions options) => writer.WriteInt32(value);
}

sealed class Int64Converter : FixedBinarySizePgConverter<long>
{
    protected override byte BinarySize => sizeof(long);
    public override long Read(PgReader reader, PgConverterOptions options) => reader.ReadInt64();
    public override void Write(PgWriter writer, long value, PgConverterOptions options) => writer.WriteInt64(value);
}

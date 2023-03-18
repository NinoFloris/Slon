using System.Text;

namespace Slon.Pg.Converters;

sealed class Int16Converter : PgFixedBinarySizeConverter<short>
{
    readonly Encoding _textEncoding;
    public Int16Converter(PgConverterOptions options) => _textEncoding = options.TextEncoding;

    // public override bool CanConvert(DataFormat format) => format is DataFormat.Binary or DataFormat.Text;
    //
    // public override ValueSize GetSize(SizeContext context, short value, ref object? writeState)
    //     => context.Format is DataFormat.Text ? _textEncoding.GetByteCount(value.ToString()) : BinarySize;

    protected override byte BinarySize => sizeof(short);
    protected override short ReadCore(PgReader reader) => reader.ReadInt16();
    // {
    //     if (reader.Format is DataFormat.Binary)
    //         return 
    //
    //     return short.Parse(_textEncoding.GetChars(reader.ReadExact(reader.ByteCount).ToArray()).AsSpan().ToString());
    // }
    //
    // public override ValueTask<short> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
    // {
    //     if (reader.Format is DataFormat.Binary)
    //         return new(Read(reader));
    //
    //     // *Imagine async work for text here*
    //     return new(short.Parse(_textEncoding.GetChars(reader.ReadExact(reader.ByteCount).ToArray()).AsSpan().ToString()));
    // }

    public override void Write(PgWriter writer, short value) => writer.WriteInt16(value);
}

sealed class Int32Converter : PgFixedBinarySizeConverter<int>
{
    protected override byte BinarySize => sizeof(int);
    protected override int ReadCore(PgReader reader) => reader.ReadInt32();
    public override void Write(PgWriter writer, int value) => writer.WriteInt32(value);
}

sealed class Int64Converter : PgFixedBinarySizeConverter<long>
{
    protected override byte BinarySize => sizeof(long);
    protected override long ReadCore(PgReader reader) => reader.ReadInt64();
    public override void Write(PgWriter writer, long value) => writer.WriteInt64(value);
}

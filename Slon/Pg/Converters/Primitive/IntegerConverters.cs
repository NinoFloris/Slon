using System;
using System.Buffers;
using System.Text;

namespace Slon.Pg.Converters;

sealed class Int16Converter : PgBufferedConverter<short>
{
    readonly Encoding _textEncoding;
    public Int16Converter(PgConverterOptions options) => _textEncoding = options.TextEncoding;

    public override bool CanConvert(DataFormat format) => format is DataFormat.Binary or DataFormat.Text;

    protected override short ReadCore(PgReader reader)
    {
        if (reader.Format is DataFormat.Binary)
            return reader.ReadInt16();

        return short.Parse(_textEncoding.GetChars(reader.ReadExact(reader.ByteCount).ToArray()).AsSpan().ToString());
    }

    public override ValueSize GetSize(ref SizeContext context, short value)
        => context.Format is DataFormat.Text ? _textEncoding.GetByteCount(value.ToString()) : sizeof(short);

    public override void Write(PgWriter writer, short value) => writer.WriteInt16(value);
}

sealed class Int32Converter : PgBufferedConverter<int>
{
    protected override int ReadCore(PgReader reader) => reader.ReadInt32();
    public override ValueSize GetSize(ref SizeContext context, int value) => sizeof(int);
    public override void Write(PgWriter writer, int value) => writer.WriteInt32(value);
}

sealed class Int64Converter : PgBufferedConverter<long>
{
    protected override long ReadCore(PgReader reader) => reader.ReadInt64();
    public override ValueSize GetSize(ref SizeContext context, long value) => sizeof(long);
    public override void Write(PgWriter writer, long value) => writer.WriteInt64(value);
}

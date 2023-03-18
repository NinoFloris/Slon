using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg.Converters;

sealed class Int16Converter : PgFixedBinarySizeConverter<short>
{
    public override bool CanConvert(DataFormat format) => format is DataFormat.Binary or DataFormat.Text;

    public override ValueSize GetSize(short value, ref object? writeState, SizeContext context, PgConverterOptions options)
        => context.Format is DataFormat.Text ? options.TextEncoding.GetByteCount(value.ToString()) : BinarySize;

    protected override byte BinarySize => sizeof(short);
    protected override short ReadCore(PgReader reader, PgConverterOptions options)
    {
        if (reader.Format is DataFormat.Binary)
            return reader.ReadInt16();

        return short.Parse(options.TextEncoding.GetChars(reader.ReadExact(reader.ByteCount).ToArray()).AsSpan().ToString());
    }

    public override ValueTask<short> ReadAsync(PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default)
    {
        if (reader.Format is DataFormat.Binary)
            return new(Read(reader, options));

        // *Imagine async work for text here*
        return new(short.Parse(options.TextEncoding.GetChars(reader.ReadExact(reader.ByteCount).ToArray()).AsSpan().ToString()));
    }

    public override void Write(PgWriter writer, short value, PgConverterOptions options) => writer.WriteInt16(value);
}

sealed class Int32Converter : PgFixedBinarySizeConverter<int>
{
    protected override byte BinarySize => sizeof(int);
    protected override int ReadCore(PgReader reader, PgConverterOptions options) => reader.ReadInt32();
    public override void Write(PgWriter writer, int value, PgConverterOptions options) => writer.WriteInt32(value);
}

sealed class Int64Converter : PgFixedBinarySizeConverter<long>
{
    protected override byte BinarySize => sizeof(long);
    protected override long ReadCore(PgReader reader, PgConverterOptions options) => reader.ReadInt64();
    public override void Write(PgWriter writer, long value, PgConverterOptions options) => writer.WriteInt64(value);
}

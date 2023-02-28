using System.Buffers;
using Slon.Protocol;

namespace Slon.Pg.Converters;

sealed class Int2Converter : PgConverter<short>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out short value, PgConverterOptions options)
        => !reader.TryReadBigEndian(out value) ? ReadStatus.NeedMoreData : ReadStatus.Done;

    public override SizeResult GetSize(short value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options)
        => SizeResult.Create(sizeof(short), fixedSize: true);
    public override void Write(PgWriter writer, short value, PgConverterOptions options)
        => writer.WriteInteger(value);
}

sealed class Int4Converter : PgConverter<int>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out int value, PgConverterOptions options)
        => !reader.TryReadBigEndian(out value) ? ReadStatus.NeedMoreData : ReadStatus.Done;

    public override SizeResult GetSize(int value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options)
        => SizeResult.Create(sizeof(int), fixedSize: true);
    public override void Write(PgWriter writer, int value, PgConverterOptions options)
        => writer.WriteInteger(value);
}

sealed class Int8Converter : PgConverter<long>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out long value, PgConverterOptions options)
        => !reader.TryReadBigEndian(out value) ? ReadStatus.NeedMoreData : ReadStatus.Done;

    public override SizeResult GetSize(long value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options)
        => SizeResult.Create(sizeof(long), fixedSize: true);
    public override void Write(PgWriter writer, long value, PgConverterOptions options)
        => writer.WriteInteger(value);
}

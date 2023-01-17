using System.Buffers;
using System.Runtime.CompilerServices;
using Slon.Buffers;
using Slon.Protocol;

namespace Slon.Pg.Converters;

sealed class BooleanConverter : PgConverter<bool>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out bool value, PgConverterOptions options)
        => reader.TryReadBool(out value).ToReadStatus();

    public override SizeResult GetSize(bool value, int bufferLength, ref object? writeState, PgConverterOptions options)
        => SizeResult.Create(sizeof(byte), fixedSize: true);
    public override void Write(PgWriter writer, bool value, PgConverterOptions options)
        => writer.WriteByte(Unsafe.As<bool, byte>(ref value));
}

sealed class ByteConverter : PgConverter<byte>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out byte value, PgConverterOptions options)
        => !reader.TryRead(out value) ? ReadStatus.NeedMoreData : ReadStatus.Done;

    public override SizeResult GetSize(byte value, int bufferLength, ref object? writeState, PgConverterOptions options)
        => SizeResult.Create(sizeof(byte), fixedSize: true);
    public override void Write(PgWriter writer, byte value, PgConverterOptions options)
        => writer.WriteByte(value);
}

sealed class SByteConverter : PgConverter<sbyte>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out sbyte value, PgConverterOptions options)
        => !reader.TryRead(out value) ? ReadStatus.NeedMoreData : ReadStatus.Done;

    public override SizeResult GetSize(sbyte value, int bufferLength, ref object? writeState, PgConverterOptions options)
        => SizeResult.Create(sizeof(sbyte), fixedSize: true);
    public override void Write(PgWriter writer, sbyte value, PgConverterOptions options)
        => writer.WriteByte((byte)value);
}

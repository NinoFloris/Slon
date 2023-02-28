using System;
using System.Buffers;
using Slon.Protocol;

namespace Slon.Pg.Converters;

sealed class DateTimeTimestampConverter: PgConverter<DateTime>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out DateTime value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    public override SizeResult GetSize(DateTime value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(PgWriter writer, DateTime value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }
}

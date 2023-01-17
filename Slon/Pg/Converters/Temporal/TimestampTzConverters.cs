using System;
using System.Buffers;
using Slon.Protocol;

namespace Slon.Pg.Converters;

sealed class DateTimeTimestampTzConverter: PgConverter<DateTime>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out DateTime value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    public override SizeResult GetSize(DateTime value, int bufferLength, ref object? writeState, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(PgWriter writer, DateTime value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }
}

sealed class DateTimeOffsetTimestampTzConverter: PgConverter<DateTimeOffset>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out DateTimeOffset value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    public override SizeResult GetSize(DateTimeOffset value, int bufferLength, ref object? writeState, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(PgWriter writer, DateTimeOffset value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }
}

sealed class LongTimestampTzConverter: PgConverter<long>
{
    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out long value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    public override SizeResult GetSize(long value, int bufferLength, ref object? writeState, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(PgWriter writer, long value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }
}

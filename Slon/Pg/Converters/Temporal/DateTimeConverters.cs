using System;

namespace Slon.Pg.Converters;

sealed class DateTimeConverter: PgFixedBinarySizeConverter<DateTime>
{
    readonly DateTimeKind _kind;
    public DateTimeConverter(DateTimeKind kind) => _kind = kind;

    protected override byte BinarySize => sizeof(long);
    protected override DateTime ReadCore(PgReader reader, PgConverterOptions options)
        => PgTimestamp.Decode(reader.ReadInt64(), _kind, options.EnableDateTimeInfinityConversions);
    public override void Write(PgWriter writer, DateTime value, PgConverterOptions options)
        => writer.WriteInt64(PgTimestamp.Encode(value, options.EnableDateTimeInfinityConversions));
}

sealed class DateTimeOffsetConverter: PgFixedBinarySizeConverter<DateTimeOffset>
{
    protected override byte BinarySize => sizeof(long);
    protected override DateTimeOffset ReadCore(PgReader reader, PgConverterOptions options)
        => PgTimestamp.Decode(reader.ReadInt64(), DateTimeKind.Utc, options.EnableDateTimeInfinityConversions);
    public override void Write(PgWriter writer, DateTimeOffset value, PgConverterOptions options)
        => writer.WriteInt64(PgTimestamp.Encode(value.DateTime, options.EnableDateTimeInfinityConversions));
}

using System;

namespace Slon.Pg.Converters;

sealed class DateTimeConverter: PgBufferedConverter<DateTime>
{
    readonly bool _dateTimeInfinityConversions;
    readonly DateTimeKind _kind;

    public DateTimeConverter(bool dateTimeInfinityConversions, DateTimeKind kind)
    {
        _dateTimeInfinityConversions = dateTimeInfinityConversions;
        _kind = kind;
    }

    protected override DateTime ReadCore(PgReader reader)
        => PgTimestamp.Decode(reader.ReadInt64(), _kind, _dateTimeInfinityConversions);

    public override ValueSize GetSize(ref SizeContext context, DateTime value) => sizeof(long);
    public override void Write(PgWriter writer, DateTime value)
        => writer.WriteInt64(PgTimestamp.Encode(value, _dateTimeInfinityConversions));
}

sealed class DateTimeOffsetConverter: PgBufferedConverter<DateTimeOffset>
{
    readonly bool _dateTimeInfinityConversions;
    public DateTimeOffsetConverter(bool dateTimeInfinityConversions) =>
        _dateTimeInfinityConversions = dateTimeInfinityConversions;

    protected override DateTimeOffset ReadCore(PgReader reader)
        => PgTimestamp.Decode(reader.ReadInt64(), DateTimeKind.Utc, _dateTimeInfinityConversions);

    public override ValueSize GetSize(ref SizeContext context, DateTimeOffset value) => sizeof(long);
    public override void Write(PgWriter writer, DateTimeOffset value)
        => writer.WriteInt64(PgTimestamp.Encode(value.DateTime, _dateTimeInfinityConversions));
}

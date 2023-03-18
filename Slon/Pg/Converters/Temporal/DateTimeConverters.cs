using System;

namespace Slon.Pg.Converters;

sealed class DateTimeConverter: PgBufferedConverter<DateTime>
{
    readonly bool _enabledDateTimeInfinityConversions;
    readonly DateTimeKind _kind;

    public DateTimeConverter(PgConverterOptions options, DateTimeKind kind)
    {
        _enabledDateTimeInfinityConversions = options.EnableDateTimeInfinityConversions;
        _kind = kind;
    }

    protected override DateTime ReadCore(PgReader reader)
        => PgTimestamp.Decode(reader.ReadInt64(), _kind, _enabledDateTimeInfinityConversions);

    public override ValueSize GetSize(ref SizeContext context, DateTime value) => sizeof(long);
    public override void Write(PgWriter writer, DateTime value)
        => writer.WriteInt64(PgTimestamp.Encode(value, _enabledDateTimeInfinityConversions));
}

sealed class DateTimeOffsetConverter: PgBufferedConverter<DateTimeOffset>
{
    readonly bool _enabledDateTimeInfinityConversions;
    public DateTimeOffsetConverter(PgConverterOptions options) =>
        _enabledDateTimeInfinityConversions = options.EnableDateTimeInfinityConversions;

    protected override DateTimeOffset ReadCore(PgReader reader)
        => PgTimestamp.Decode(reader.ReadInt64(), DateTimeKind.Utc, _enabledDateTimeInfinityConversions);

    public override ValueSize GetSize(ref SizeContext context, DateTimeOffset value) => sizeof(long);
    public override void Write(PgWriter writer, DateTimeOffset value)
        => writer.WriteInt64(PgTimestamp.Encode(value.DateTime, _enabledDateTimeInfinityConversions));
}

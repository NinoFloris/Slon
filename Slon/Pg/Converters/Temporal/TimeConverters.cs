using System;

namespace Slon.Pg.Converters;

sealed class TimeSpanTimeConverter : PgBufferedConverter<TimeSpan>
{
    protected override TimeSpan ReadCore(PgReader reader) => new(reader.ReadInt64() * 10);
    public override ValueSize GetSize(ref SizeContext context, TimeSpan value) => sizeof(long);
    public override void Write(PgWriter writer, TimeSpan value) => writer.WriteInt64(value.Ticks / 10);
}

#if NET6_0_OR_GREATER
sealed class TimeOnlyTimeConverter : PgBufferedConverter<TimeOnly>
{
    protected override TimeOnly ReadCore(PgReader reader) => new(reader.ReadInt64() * 10);
    public override ValueSize GetSize(ref SizeContext context, TimeOnly value) => sizeof(long);
    public override void Write(PgWriter writer, TimeOnly value) => writer.WriteInt64(value.Ticks / 10);
}
#endif

sealed class DateTimeOffsetTimeTzConverter : PgBufferedConverter<DateTimeOffset>
{
    protected override DateTimeOffset ReadCore(PgReader reader)
    {
        var ticks = reader.ReadInt64() * 10;
        var offset = new TimeSpan(0, 0, -reader.ReadInt32());
        return new DateTimeOffset(ticks + TimeSpan.TicksPerDay, offset);
    }
    public override ValueSize GetSize(ref SizeContext context, DateTimeOffset value) => sizeof(long) + sizeof(int);
    public override void Write(PgWriter writer, DateTimeOffset value)
    {
        writer.WriteInt64(value.Ticks / 10);
        writer.WriteInt32(-(int)(value.Offset.Ticks / TimeSpan.TicksPerSecond));
    }
}

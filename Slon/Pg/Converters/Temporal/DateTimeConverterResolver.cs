using System;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg.Converters;

sealed class DateTimeConverterResolver: PgConverterResolver<DateTime>
{
    readonly PgTypeId _timestampTz;
    readonly PgTypeId _timestamp;
    PgConverter<DateTime>? _converter;
    PgConverter<DateTime>? _tzConverter;

    public DateTimeConverterResolver(PgTypeId timestampTz, PgTypeId timestamp)
    {
        _timestampTz = timestampTz;
        _timestamp = timestamp;
    }

    public override PgConverter<DateTime> GetConverter(DateTime value)
        => value.Kind is DateTimeKind.Utc
            ? _tzConverter ??= new DateTimeTimestampTzConverter()
            : _converter ??= new DateTimeTimestampConverter();

    public override PgConverter<DateTime> GetConverter(Field field) => _converter ??= new DateTimeTimestampConverter();
    public override PgTypeId GetPgTypeId(DateTime value) => value.Kind is DateTimeKind.Utc ? _timestampTz : _timestamp;
}

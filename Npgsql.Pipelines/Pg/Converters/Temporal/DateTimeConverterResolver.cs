using System;
using Npgsql.Pipelines.Pg.Types;

namespace Npgsql.Pipelines.Pg.Converters;

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
    {
        if (value.Kind is DateTimeKind.Utc)
            return _tzConverter ??= new DateTimeTimestampTzConverter();

        return _converter ??= new DateTimeTimestampConverter();
    }

    public override PgTypeId GetDataTypeName(DateTime value) => value.Kind is DateTimeKind.Utc ? _timestampTz : _timestamp;
}

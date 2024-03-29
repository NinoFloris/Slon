using System;
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

    public override PgConverterResolution<DateTime> GetDefault(PgTypeId pgTypeId)
    {
        if (pgTypeId == _timestampTz)
            return new(_tzConverter ??= new DateTimeConverter(DateTimeKind.Utc), _timestampTz);
        if (pgTypeId == _timestamp)
            return new(_converter ??= new DateTimeConverter(DateTimeKind.Unspecified), _timestamp); 

        throw CreateUnsupportedPgTypeIdException(pgTypeId);
    }

    public override PgConverterResolution<DateTime> Get(DateTime value, PgTypeId? expectedPgTypeId)
    {
        if (value.Kind is DateTimeKind.Utc)
        {
            if (expectedPgTypeId == _timestamp)
                throw new ArgumentException(
                    "Cannot write DateTime with Kind=UTC to PostgreSQL type 'timestamp without time zone', " +
                    "consider using 'timestamp with time zone'. " +
                    "Note that it's not possible to mix DateTimes with different Kinds in an array/range.", nameof(value));

            // We coalesce with expectedPgTypeId to throw on unknown type ids.
            return GetDefault(expectedPgTypeId ?? _timestampTz);
        }

        if (expectedPgTypeId == _timestampTz)
            throw new ArgumentException(
                $"Cannot write DateTime with Kind={value.Kind} to PostgreSQL type 'timestamp with time zone', only UTC is supported. " +
                "Note that it's not possible to mix DateTimes with different Kinds in an array/range. ", nameof(value));

        // We coalesce with expectedPgTypeId to throw on unknown type ids.
        return GetDefault(expectedPgTypeId ?? _timestamp);
    }
}

sealed class DateTimeOffsetUtcOnlyConverterResolver: PgConverterResolver<DateTimeOffset>
{
    readonly PgTypeId _timestampTz;
    readonly PgConverter<DateTimeOffset> _converter = new DateTimeOffsetConverter();

    public DateTimeOffsetUtcOnlyConverterResolver(PgTypeId timestampTz) => _timestampTz = timestampTz;

    public override PgConverterResolution<DateTimeOffset> GetDefault(PgTypeId pgTypeId)
        => pgTypeId == _timestampTz
            ? new(_converter, _timestampTz)
            : throw CreateUnsupportedPgTypeIdException(pgTypeId);

    public override PgConverterResolution<DateTimeOffset> Get(DateTimeOffset value, PgTypeId? expectedPgTypeId)
    {
        // We run GetDefault first to make sure the expectedPgTypeId is known.
        var resolution = GetDefault(expectedPgTypeId ?? _timestampTz);
        return value.Offset == TimeSpan.Zero
            ? resolution
            : throw new ArgumentException(
                $"Cannot write DateTimeOffset with Offset={value.Offset} to PostgreSQL type 'timestamp with time zone', only offset 0 (UTC) is supported. ",
                nameof(value));
    }
}

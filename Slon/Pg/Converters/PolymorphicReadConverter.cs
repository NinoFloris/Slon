using System;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg.Converters;

// TODO would we ever support polymorphic writing, why?
abstract class PolymorphicReadConverter : PgStreamingConverter<object>
{
    protected PolymorphicReadConverter(Type effectiveType) => EffectiveType = effectiveType;

    public Type EffectiveType { get; }

    public sealed override ValueSize GetSize(ref SizeContext context, object value)
        => throw new NotSupportedException("Polymorphic writing is not supported.");

    public sealed override void Write(PgWriter writer, object value)
        => throw new NotSupportedException("Polymorphic writing is not supported.");

    public sealed override ValueTask WriteAsync(PgWriter writer, object value, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("Polymorphic writing is not supported.");
}

abstract class PolymorphicReadConverterResolver : PgConverterResolver<object>
{
    protected PolymorphicReadConverterResolver(PgTypeId pgTypeId) => PgTypeId = pgTypeId;

    protected PgTypeId PgTypeId { get; }

    protected abstract PolymorphicReadConverter Get(Field? field);

    public sealed override PgConverterResolution<object> GetDefault(PgTypeId pgTypeId)
    {
        if (pgTypeId != PgTypeId)
            throw CreateUnsupportedPgTypeIdException(pgTypeId);

        var converter = Get(null);
        return new(converter, PgTypeId, converter.EffectiveType);
    }

    public sealed override PgConverterResolution<object> Get(object? value, PgTypeId? expectedPgTypeId)
        => throw new NotSupportedException("Polymorphic writing is not supported, try to resolve a converter by the type of an actual value instead.");

    public sealed override PgConverterResolution<object> Get(Field field)
    {
        if (field.PgTypeId != PgTypeId)
            throw CreateUnsupportedPgTypeIdException(field.PgTypeId);

        var converter = Get(field);
        return new(converter, PgTypeId, converter.EffectiveType);
    }
}

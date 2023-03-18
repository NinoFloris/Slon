using System;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Types;

namespace Slon.Pg;

// TODO should this even be inheriting from PgConverter? (when do we want to be able to add them or use them interchangeably?)
abstract class PgConverterFactory : PgConverter
{
    public abstract PgConverterInfo? CreateConverterInfo(Type type, PgConverterOptions options, PgTypeId? pgTypeId = null);

    public sealed override bool CanConvert(DataFormat format) => false;
    internal sealed override bool IsDbNullable => throw new NotSupportedException();

    internal sealed override Type TypeToConvert => throw new NotSupportedException();
    internal sealed override object? ReadAsObject(PgReader reader, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override ValueTask<object?> ReadAsObjectAsync(PgReader reader, PgConverterOptions options, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    internal sealed override bool IsDbNullValueAsObject(object? value, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override ValueSize GetSizeAsObject(object value, ref object? writeState, SizeContext context, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override void WriteAsObject(PgWriter writer, object? value, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override ValueTask WriteAsObjectAsync(PgWriter writer, object? value, PgConverterOptions options, CancellationToken cancellationToken = default) => throw new NotSupportedException();
}

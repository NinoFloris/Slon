using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Types;
using Slon.Protocol;

namespace Slon.Pg;

abstract class PgConverterFactory : PgConverter
{
    [RequiresUnreferencedCode("Reflection used for pg type conversions.")]
    public abstract PgConverterInfo? CreateConverterInfo(Type type, PgConverterOptions options, PgTypeId? pgTypeId = null);

    public sealed override bool CanConvert(DataRepresentation representation) => false;

    internal sealed override bool IsDbNullValueAsObject(object? value, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override SizeResult GetSizeAsObject(object value, int bufferLength, ref object? writeState, DataRepresentation representation, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override ReadStatus ReadAsObject(ref SequenceReader<byte> reader, int byteCount, out object? value, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override void WriteAsObject(PgWriter writer, object? value, PgConverterOptions options) => throw new NotSupportedException();
    internal sealed override ValueTask WriteAsObjectAsync(PgWriter writer, object? value, PgConverterOptions options, CancellationToken cancellationToken = default) => throw new NotSupportedException();
}

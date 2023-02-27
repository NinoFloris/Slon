using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Types;
using Slon.Protocol;

namespace Slon.Pg.Converters;

class ArrayConverter<T> : PgConverter<T?[]>
{
    readonly PgConverter<T> _elemConverter;
    readonly PgTypeId _elemTypeId;
    readonly ArrayPool<(SizeResult, object?)> _statePool;
    readonly bool _elemTypeNullable;
    readonly int _pgLowerBound;
    int? _elemFixedSize;

    public ArrayConverter(PgConverter<T> elemConverter, PgTypeId elemTypeId, ArrayPool<(SizeResult, object?)> statePool, int pgLowerBound = 1)
    {
        _elemConverter = elemConverter;
        _elemTypeId = elemTypeId;
        _elemTypeNullable = _elemConverter.IsTypeDbNullable;
        _statePool = statePool;
        _pgLowerBound = pgLowerBound;
    }

    public override bool CanConvert => _elemConverter.CanConvert;

    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out T?[] value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    int? InitFixedSize(T?[] values, PgConverterOptions options)
    {
        var firstNonNullValue = !_elemTypeNullable ? values[0] : default;
        if (_elemTypeNullable)
        {
            var elemConverter = _elemConverter;
            foreach (var v in values)
                if (!elemConverter.IsDbNullValue(v, options))
                {
                    firstNonNullValue = v;
                    break;
                }
        }

        // We couldn't check any size as no element was acceptable as a non null value for GetSize, next time maybe.
        // This should rarely happen, effectively this means sending an entire array containing NULLs exclusively.
        if (firstNonNullValue is null)
            return null;

        var elemState = (object?)null;
        var sizeResult = _elemConverter.GetSize(firstNonNullValue, 0, ref elemState, options);
        return sizeResult.Value is null || sizeResult.Kind is not SizeResultKind.FixedSize ? -1 : sizeResult.Value;
    }

    SizeResult GetElementsSize(T?[] values, int bufferLength, (SizeResult, object?)[] elementStates, PgConverterOptions options)
    {
        Debug.Assert(elementStates.Length == values.Length);
        var totalSize = SizeResult.Zero;
        var elemConverter = _elemConverter;
        var elemTypeNullable = _elemTypeNullable;
        for (var i = 0; i < values.Length; i++)
        {
            var value = values[i];
            ref var elemItem = ref elementStates[i];
            var elemState = (object?)null;
            var sizeResult =
                elemTypeNullable && _elemConverter.IsDbNullValue(value, options)
                ? SizeResult.Zero
                : elemConverter.GetSize(value!, bufferLength, ref elemState, options);

            if (sizeResult.Kind is SizeResultKind.FixedSize)
                throw new InvalidOperationException($"GetSize returning a fixed size {nameof(SizeResult)} should not decide to do so based on the passed in value, it should be static. Use a normal {nameof(SizeResult)} if that is not possible.");

            elemItem = (sizeResult, elemState);
            // Set it to zero on an unknown/null byte count.
            bufferLength -= sizeResult.Value ?? bufferLength;
            totalSize = totalSize.Combine(sizeResult);
        }
        return totalSize;
    }

    public override SizeResult GetSize(T?[] values, int bufferLength, ref object? writeState, PgConverterOptions options)
    {
        var formatSize = SizeResult.Create(
            4 +              // Dimensions
            4 +              // Flags
            4 +              // Element OID
            1 * 8 +          // Dimensions * (array length and lower bound)
            4 * values.Length // Element length integers
        );

        if (values.Length == 0)
            return formatSize;

        var fixedSize = _elemFixedSize ??= InitFixedSize(values, options);
        if (fixedSize is not null and not -1)
        {
            var nonNullValues = values.Length;
            if (_elemTypeNullable)
            {
                var nulls = 0;
                var elemConverter = _elemConverter;
                foreach (var value in values)
                {
                    if (elemConverter.IsDbNullValue(value, options))
                        nulls++;
                }

                nonNullValues -= nulls;
            }
            return formatSize.Combine(SizeResult.Create(nonNullValues * fixedSize.GetValueOrDefault()));
        }

        var stateArray = _statePool.Rent(values.Length);
        var elementsSize = GetElementsSize(values, bufferLength - formatSize.Value ?? 0, stateArray, options);
        writeState = stateArray;
        return formatSize.Combine(elementsSize);
    }

    async ValueTask WriteCore(bool async, PgWriter writer, T?[] values, PgConverterOptions options, CancellationToken cancellationToken)
    {
        if (writer.State is not (SizeResult, object?)[] state)
            state = writer.State is null && _elemFixedSize is not null and not -1 ? null! :
                throw new InvalidOperationException($"Invalid state, expected {typeof((SizeResult, object?)[]).FullName}");

        writer.WriteInteger(1); // Dimensions
        writer.WriteInteger(0); // Flags (not really used)
        writer.WriteAsOid(_elemTypeId);
        writer.WriteInteger(values.Length);
        writer.WriteInteger(_pgLowerBound);

        var elemConverter = _elemConverter;
        var elemTypeNullable = _elemTypeNullable;
        var lastState = writer.State;

        // TODO reexamine null semantics, specifically structs with a null value.

        // The test for -1 happened at the start of the method.
        if (_elemFixedSize is { } length)
            foreach (var value in values)
            {
                if (elemTypeNullable && elemConverter.IsDbNullValue(value, options))
                    writer.WriteInteger(-1);
                else
                    await WriteValue(value, length, null).ConfigureAwait(false);
            }
        else
            for (var i = 0; i < values.Length && i < state.Length; i++)
            {
                var value = values[i];
                if (elemTypeNullable && elemConverter.IsDbNullValue(value, options))
                {
                    writer.WriteInteger(-1);
                    continue;
                }

                var (sizeResult, elemState) = state[i];
                switch (sizeResult.Kind)
                {
                    case SizeResultKind.Size:
                        await WriteValue(value, sizeResult.Value.GetValueOrDefault(), elemState).ConfigureAwait(false);
                        break;
                    case SizeResultKind.UpperBound:
                        throw new NotImplementedException(); // TODO
                    case SizeResultKind.Unknown:
                        {
                            using var bufferedOutput = options.GetBufferedOutput(elemConverter!, value, elemState, DataRepresentation.Binary);
                            writer.WriteInteger(bufferedOutput.Length);
                            if (async)
                                await bufferedOutput.WriteAsync(writer, cancellationToken).ConfigureAwait(false);
                            else
                                bufferedOutput.Write(writer);
                        }
                        break;
                    case SizeResultKind.FixedSize:
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                await writer.Flush(async, cancellationToken).ConfigureAwait(false);
            }

        ValueTask WriteValue(T? value, int length, object? state)
        {
            writer.WriteInteger(length);

            if (state is not null || lastState is not null)
                writer.UpdateState(lastState = state, SizeResult.Create(length));

            if (async)
                return elemConverter.WriteAsync(writer, value!, options, cancellationToken);

            elemConverter.Write(writer, value!, options);
            return new ValueTask();
        }
    }

    public override void Write(PgWriter writer, T?[] values, PgConverterOptions options)
        => WriteCore(async: false, writer, values, options, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask WriteAsync(PgWriter writer, T?[] values, PgConverterOptions options, CancellationToken cancellationToken = default)
        => WriteCore(async: true, writer, values, options, cancellationToken);

    // TODO implement text representation then this can be _elemConverter.CanTextConvert;
    public override bool CanTextConvert => false;
}

class MultiDimArrayConverter<T, TConverter> : PgConverter<Array> where TConverter : PgConverter<T>
{
    readonly TConverter _effectiveConverter;
    protected MultiDimArrayConverter(TConverter effectiveConverter) => _effectiveConverter = effectiveConverter;

    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out Array value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    public override SizeResult GetSize(Array value, int bufferLength, ref object? writeState, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(PgWriter writer, Array value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }
}

// TODO Support icollection in general.
sealed class ArrayConverterFactory: PgConverterFactory
{
    public override PgConverterInfo? CreateConverterInfo(Type type, PgConverterOptions options, PgTypeId? pgTypeId = null)
    {
        if (!type.IsArray)
            return null;

        var elementType = type.GetElementType()!;
        var elementInfo = options.GetConverterInfo(elementType, pgTypeId is not { } id ? null : options.GetElementTypeId(id));
        if (elementInfo is null)
            throw new NotSupportedException($"Cannot convert array with element type '{elementType.FullName}', no converter registered for this element type.");

        // MAXDIM in pg is 6, `SELECT '{{{{{{{1}}}}}}}'::integer[]` does not allow the cast.

        // TODO We may want to support this through a resolver that checks whether all array values (recursively) are of the same length
        // and interpret the entire array as a pg multidim encoding, it sure beats working with multidims in C#.
        if (elementType.IsArray)
            throw new NotSupportedException("Cannot convert jagged arrays.");

        var rank = type.GetArrayRank();
        // For value dependent converters we must delay the element elementInfo work.
        // TODO fill in accurate constructor args.
        return (elementInfo is PgConverterResolverInfo, rank) switch
        {
            (false, 1) => CreateInfoFromElementInfo(elementInfo, (PgConverter)Activator.CreateInstance(typeof(ArrayConverter<>).MakeGenericType(elementType, elementInfo.ConverterType), elementInfo.Converter)!),
            (false, _) => CreateInfoFromElementInfo(elementInfo, (PgConverter)Activator.CreateInstance(typeof(MultiDimArrayConverter<,>).MakeGenericType(elementType, elementInfo.ConverterType), elementInfo.Converter)!),

            (true, 1) => CreateInfoFromElementInfo(elementInfo, (PgConverter)Activator.CreateInstance(typeof(ArrayConverterResolver<>).MakeGenericType(elementType), elementInfo)!),
            (true, _) => CreateInfoFromElementInfo(elementInfo, (PgConverter)Activator.CreateInstance(typeof(MultiDimArrayConverterResolver<>).MakeGenericType(elementType), elementInfo, rank)!)
        };

        PgConverterInfo CreateInfoFromElementInfo(PgConverterInfo elementInfo, PgConverter converter)
        {
            PgConverterInfo info;
            if (elementInfo is PgConverterResolverInfo)
                info = (PgConverterInfo)Activator.CreateInstance(typeof(PgConverterResolverInfo<>).MakeGenericType(type), options, converter);
            else
                info = (PgConverterInfo)Activator.CreateInstance(typeof(PgConverterInfo<>).MakeGenericType(type), options, converter, options.GetArrayTypeId(elementInfo.PgTypeId!.Value));

            if (elementInfo.IsDefault)
                typeof(PgConverterInfo).GetProperty("IsDefault")!.SetValue(info, elementInfo.IsDefault);

            if (elementInfo.PreferredRepresentation is not null)
                typeof(PgConverterInfo).GetProperty("PreferredRepresentation")!.SetValue(info, elementInfo.PreferredRepresentation);

            return info;
        }
    }

    // TODO benchmark this unit.
    class ArrayConverterResolver<T> : PgConverterResolver<T?[]>
    {
        readonly PgConverterResolverInfo<T> _elemResolverInfo;
        readonly PgConverterOptions _options;
        readonly ConcurrentDictionary<PgConverter<T>, PgConverter<T?[]>> _converters = new(ReferenceEqualityComparer.Instance);
        PgConverter<T>? _elemConverter;
        PgConverter<T?[]>? _converter;

        public ArrayConverterResolver(PgConverterResolverInfo<T> elemResolverInfo, PgConverterOptions options)
        {
            _elemResolverInfo = elemResolverInfo;
            _options = options;
        }

        public override PgConverter<T?[]> GetConverter(T?[]? value)
        {
            var v = value is null ? default : value[0]; // We don't need to check lower bounds here, only relevant for multi dim.
            var elemConverter = _elemResolverInfo.ConverterResolver.GetConverter(v);

            // Cache the last one used separately as well for faster recurring lookups.
            if (ReferenceEquals(elemConverter, _elemConverter))
                return _converter!;

            _elemConverter = elemConverter;
            return _converter = _converters.GetOrAdd(elemConverter, static (elemConverter, state) =>
            {
                var (elemResolverInfo, v) = state;
                return new ArrayConverter<T>(
                    elemConverter,
                    elemResolverInfo.GetPgTypeId(v),
                    elemResolverInfo.Options.GetArrayPool<(SizeResult, object?)>()
                );
            }, (_elemResolverInfo, v));
        }

        public override PgTypeId GetDataTypeName(T?[]? value)
        {
            var v = value is null ? default : value[0]; // We don't need to check lower bounds here, only relevant for multi dim.
            return _options.GetArrayTypeId(_elemResolverInfo.GetPgTypeId(v));
        }
    }

    class MultiDimArrayConverterResolver<T> : PgConverterResolver
    {
        readonly PgConverterResolverInfo _elemResolverInfo;
        readonly int _rank;

        public MultiDimArrayConverterResolver(PgConverterResolverInfo elemResolverInfo, int rank): base(typeof(Array))
        {
            _elemResolverInfo = elemResolverInfo;
            _rank = rank;
        }

        public override PgConverter GetConverterAsObject(object? value)
        {
            throw new NotImplementedException();
        }

        public override PgTypeId GetDataTypeNameAsObject(object? value)
        {
            throw new NotImplementedException();
        }
    }
}

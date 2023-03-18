using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg.Converters;

readonly struct ArrayConverter
{
    readonly IArrayElementOperations _elementOperations;
    public PgTypeId ElemTypeId { get; }
    readonly ArrayPool<(ValueSize, object?)> _statePool;
    readonly bool _elemTypeDbNullable;
    readonly int _pgLowerBound;

    public ArrayConverter(IArrayElementOperations elementOperations, bool elemTypeDbNullable, PgTypeId elemTypeId, ArrayPool<(ValueSize, object?)> statePool, int pgLowerBound = 1)
    {
        ElemTypeId = elemTypeId;
        _statePool = statePool;
        _elemTypeDbNullable = elemTypeDbNullable;
        _pgLowerBound = pgLowerBound;
        _elementOperations = elementOperations;
    }

    ValueSize GetElemsSize(Array values, int bufferLength, (ValueSize, object?)[] elementStates, DataFormat format)
    {
        Debug.Assert(elementStates.Length == values.Length);
        var totalSize = ValueSize.Zero;
        var elemTypeNullable = _elemTypeDbNullable;
        for (var i = 0; i < values.Length; i++)
        {
            ref var elemItem = ref elementStates[i];
            var elemState = (object?)null;
            var sizeResult =
                elemTypeNullable && _elementOperations.IsDbNullValue(values, i)
                ? ValueSize.Zero
                : _elementOperations.GetSize(new(format, bufferLength), values, i, ref elemState);

            elemItem = (sizeResult, elemState);
            // Set it to zero on an unknown/null byte count.
            bufferLength -= sizeResult.Value ?? bufferLength;
            totalSize = totalSize.Combine(sizeResult);
        }
        return totalSize;
    }

    ValueSize GetFixedElemsSize(Array values, DataFormat format)
    {
        var discardedElemState = (object?)null;
        var fixedSize = _elementOperations.GetSize(new(format, 0), values, 0, ref discardedElemState).Value;
        var nonNullValues = values.Length;
        if (_elemTypeDbNullable)
        {
            var nulls = 0;
            for (var i = 0; i < values.Length; i++)
            {
                if (_elementOperations.IsDbNullValue(values, i))
                    nulls++;
            }

            nonNullValues -= nulls;
        }

        return ValueSize.Create(nonNullValues * fixedSize.GetValueOrDefault());
    }

    public ValueSize GetSize(SizeContext context, Array values, ref object? writeState)
    {
        var formatSize = ValueSize.Create(
            4 + // Dimensions
            4 + // Flags
            4 + // Element OID
            1 * 8 + // Dimensions * (array length and lower bound)
            4 * values.Length // Element length integers
        );

        if (values.Length == 0)
            return formatSize;

        ValueSize elemsSize;
        if (_elementOperations.HasFixedSize(context.Format))
        {
            elemsSize = GetFixedElemsSize(values, context.Format);
            writeState = Array.Empty<(ValueSize, object?)>();
        }
        else
        {
            var stateArray = _statePool.Rent(values.Length);
            elemsSize = GetElemsSize(values, context.BufferLength - formatSize.Value ?? 0, stateArray, context.Format);
            writeState = stateArray;
        }

        return formatSize.Combine(elemsSize);
    }

    public async ValueTask WriteCore(bool async, PgWriter writer, Array values, CancellationToken cancellationToken)
    {
        if (writer.State is not (ValueSize, object?)[] state)
            throw new InvalidOperationException($"Invalid state, expected {typeof((ValueSize, object?)[]).FullName}.");

        writer.WriteInt32(1); // Dimensions
        writer.WriteInt32(0); // Flags (not really used)
        writer.WriteAsOid(ElemTypeId);
        writer.WriteInt32(values.Length);
        writer.WriteInt32(_pgLowerBound);

        if (values.Length is 0)
            return;

        var elemTypeDbNullable = _elemTypeDbNullable;
        var lastState = writer.State;

        // Fixed size path, we don't store anything.
        if (state.Length is 0)
        {
            var discardedElemState = (object?)null;
            var length = _elementOperations.GetSize(new(writer.DataFormat, 0), values, 0, ref discardedElemState).Value.GetValueOrDefault();
            for (var i = 0; i < values.Length; i++)
            {
                if (elemTypeDbNullable && _elementOperations.IsDbNullValue(values, i))
                    writer.WriteInt32(-1);
                else
                    await WriteValue(_elementOperations, i, length, null).ConfigureAwait(false);
            }
        }
        else
            for (var i = 0; i < values.Length && i < state.Length; i++)
            {
                if (elemTypeDbNullable && _elementOperations.IsDbNullValue(values, i))
                {
                    writer.WriteInt32(-1);
                    continue;
                }

                var (sizeResult, elemState) = state[i];
                switch (sizeResult.Kind)
                {
                    case ValueSizeKind.Size:
                        await WriteValue(_elementOperations, i, sizeResult.Value.GetValueOrDefault(), elemState).ConfigureAwait(false);
                        break;
                    case ValueSizeKind.UpperBound:
                        throw new NotImplementedException(); // TODO
                    case ValueSizeKind.Unknown:
                        throw new NotImplementedException();
                        // {
                        //     using var bufferedOutput = options.GetBufferedOutput(elemConverter!, value, elemState, DataRepresentation.Binary);
                        //     writer.WriteInt32(bufferedOutput.Length);
                        //     if (async)
                        //         await bufferedOutput.WriteAsync(writer, cancellationToken).ConfigureAwait(false);
                        //     else
                        //         bufferedOutput.Write(writer);
                        // }
                        // break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                await writer.Flush(async, cancellationToken).ConfigureAwait(false);
            }

        ValueTask WriteValue(IArrayElementOperations elementOps, int index, int length, object? state)
        {
            writer.WriteInt32(length);

            if (state is not null || lastState is not null)
                writer.UpdateState(lastState = state, ValueSize.Create(length));

            if (async)
                return elementOps.WriteAsync(writer, values, index, cancellationToken);

            elementOps.Write(writer, values, index);
            return new ValueTask();
        }
    }
}

interface IArrayElementOperations
{
    bool HasFixedSize(DataFormat format);
    ValueSize GetSize(SizeContext context, Array array, int index, ref object? writeState);
    bool IsDbNullValue(Array array, int index);
    void Write(PgWriter writer, Array array, int index);
    ValueTask WriteAsync(PgWriter writer, Array array, int index, CancellationToken cancellationToken = default);
}

sealed class ArrayConverter<T> : PgConverter<T?[]>, IArrayElementOperations
{
    readonly PgConverter<T> _elemConverter;
    readonly ArrayConverter _arrayConverter;

    public ArrayConverter(PgConverterResolution<T> resolution, ArrayPool<(ValueSize, object?)> statePool, int pgLowerBound = 1)
    {
        _elemConverter = resolution.Converter;
        _arrayConverter = new ArrayConverter(this, _elemConverter.IsDbNullable, resolution.PgTypeId, statePool, pgLowerBound);
    }

    internal PgTypeId ElemTypeId => _arrayConverter.ElemTypeId;

    public override bool CanConvert(DataFormat format) => _elemConverter.CanConvert(format);

    public override T?[] Read(PgReader reader)
    {
        throw new NotImplementedException();
    }

    public override ValueSize GetSize(SizeContext context, T?[] values, ref object? writeState)
        => _arrayConverter.GetSize(context, values, ref writeState);

    public override void Write(PgWriter writer, T?[] values)
        => _arrayConverter.WriteCore(async: false, writer, values, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask WriteAsync(PgWriter writer, T?[] values, CancellationToken cancellationToken = default)
        => _arrayConverter.WriteCore(async: true, writer, values, cancellationToken);

    ValueSize IArrayElementOperations.GetSize(SizeContext context, Array array, int index, ref object? writeState)
        => _elemConverter.GetSize(context, Unsafe.As<Array, T?[]>(ref array)[index]!, ref writeState);

    bool IArrayElementOperations.IsDbNullValue(Array array, int index)
        => _elemConverter.IsDbNullValue(Unsafe.As<Array, T?[]>(ref array)[index]);

    void IArrayElementOperations.Write(PgWriter writer, Array array, int index)
        => _elemConverter.Write(writer, Unsafe.As<Array, T?[]>(ref array)[index]!);

    ValueTask IArrayElementOperations.WriteAsync(PgWriter writer, Array array, int index, CancellationToken cancellationToken)
        => _elemConverter.WriteAsync(writer, Unsafe.As<Array, T?[]>(ref array)[index]!, cancellationToken);
}

class MultiDimArrayConverter<TElement, T> : PgConverter<T>
{
    readonly PgConverter<TElement> _elementConverter;
    protected MultiDimArrayConverter(PgConverterResolution<TElement> resolution, int rank) => _elementConverter = resolution.Converter;

    public override T Read(PgReader reader)
    {
        throw new NotImplementedException();
    }

    public override ValueSize GetSize(SizeContext context, T value, ref object? writeState)
    {
        throw new NotImplementedException();
    }

    public override void Write(PgWriter writer, T value)
    {
        throw new NotImplementedException();
    }
}
// // TODO Support icollection in general.
// sealed class ArrayConverterFactory: PgConverterFactory
// {
//     [RequiresUnreferencedCode("Reflection used for pg type conversions.")]
//     public override PgConverterInfo? CreateConverterInfo(Type type, PgConverterOptions options, PgTypeId? pgTypeId = null)
//     {
//         if (!type.IsArray)
//             return null;
//
//         var elementType = type.GetElementType()!;
//         var elementInfo = options.GetConverterInfo(elementType, pgTypeId is not { } id ? null : options.GetElementTypeId(id));
//         if (elementInfo is null)
//             throw new NotSupportedException($"Cannot convert array with element type '{elementType.FullName}', no converter registered for this element type.");
//
//         // MAXDIM in pg is 6, `SELECT '{{{{{{{1}}}}}}}'::integer[]` does not allow the cast.
//
//         // TODO We may want to support this through a resolver that checks whether all array values (recursively) are of the same length
//         // and interpret the entire array as a pg multidim encoding, it sure beats working with multidims in C#.
//         if (elementType.IsArray)
//             throw new NotSupportedException("Cannot convert jagged arrays.");
//
//         var rank = type.GetArrayRank();
//         // For value dependent converters we must delay the element elementInfo work.
//         var arrayPgTypeId = pgTypeId ?? (elementInfo.PgTypeId is { } elemId ? options.GetArrayTypeId(elemId) : null);
//         return (elementInfo.IsValueDependent, rank) switch
//         {
//             (false, 1) => elementInfo.Compose((PgConverter)Activator.CreateInstance(typeof(ArrayConverter<>).MakeGenericType(elementType), elementInfo.GetResolution(null, elementInfo.PgTypeId), options.GetArrayPool<(ValueSize, object?)>(), 1)!, arrayPgTypeId.GetValueOrDefault()),
//             (false, _) => elementInfo.Compose((PgConverter)Activator.CreateInstance(typeof(MultiDimArrayConverter<,>).MakeGenericType(elementType, type), elementInfo.GetResolution(null, elementInfo.PgTypeId), rank)!, arrayPgTypeId.GetValueOrDefault()),
//
//             (true, 1) => elementInfo.Compose((PgConverterResolver)Activator.CreateInstance(typeof(ArrayConverterResolver<>).MakeGenericType(elementType), elementInfo)!, arrayPgTypeId),
//             (true, _) => elementInfo.Compose((PgConverterResolver)Activator.CreateInstance(typeof(MultiDimArrayConverterResolver<,>).MakeGenericType(elementType, type), elementInfo, rank)!, arrayPgTypeId)
//         };
//     }
// }

// TODO benchmark this unit.
sealed class ArrayConverterResolver<T> : PgConverterResolver<T?[]>
{
    readonly PgConverterInfo _elemConverterInfo;
    readonly ConcurrentDictionary<PgConverter<T>, ArrayConverter<T>> _converters = new(ReferenceEqualityComparer.Instance);
    PgConverter<T>? _lastElemConverter;
    PgConverterResolution<T?[]> _lastResolution;

    public ArrayConverterResolver(PgConverterInfo elemConverterInfo)
    {
        _elemConverterInfo = elemConverterInfo;
    }

    // We don't need to check lower bounds here, only relevant for multi dim.
    T? GetValueOrDefault(T?[]? values) => values is null ? default : values[0];

    // TODO improve, much more should be cached (including array/element type id mappings).
    public override PgConverterResolution<T?[]> GetDefault(PgTypeId pgTypeId) => Get(Array.Empty<T?>(), pgTypeId);
    public override PgConverterResolution<T?[]> Get(T?[]? values, PgTypeId? expectedPgTypeId)
    {
        var valueOrDefault = GetValueOrDefault(values);
        // We get the pg type id for the first element to be able to pass it in for the subsequent, per element, calls of GetConverter.
        // This is how we allow resolvers to catch value inconsistencies that would cause converter mixing and return useful error messages.
        var elementTypeId = expectedPgTypeId is { } id ? (PgTypeId?)_elemConverterInfo.Options.GetElementTypeId(id) : null;
        var expectedResolution = _elemConverterInfo.GetResolution(valueOrDefault, elementTypeId);
        foreach (var value in values ?? Array.Empty<T?>())
            _elemConverterInfo.GetResolution(value, expectedResolution.PgTypeId);

        // Cache the last one used separately as well for faster recurring lookups.
        if (ReferenceEquals(expectedResolution.Converter, _lastElemConverter))
            return _lastResolution;

        var converter = _converters.GetOrAdd(expectedResolution.Converter, static (elemConverter, state) =>
        {
            var (elemResolverInfo, expectedElemPgTypeId) = state;
            return new ArrayConverter<T>(
                new(elemConverter, expectedElemPgTypeId),
                elemResolverInfo.Options.GetArrayPool<(ValueSize, object?)>()
            );
        }, (_elemConverterInfo, expectedResolution.PgTypeId));

        // TODO remove, just key on the entire resolution.
        if (converter.ElemTypeId != expectedResolution.PgTypeId)
            throw new InvalidOperationException("Type id mismatch.");

        _lastElemConverter = expectedResolution.Converter;
        return _lastResolution = new PgConverterResolution<T?[]>(converter, _elemConverterInfo.Options.GetArrayTypeId(expectedResolution.PgTypeId));
    }

    public override PgConverterResolution<T?[]> Get(Field field)
    {
        throw new NotImplementedException();
    }
}

sealed class MultiDimArrayConverterResolver<TElement, T> : PgConverterResolver<T>
{
    readonly PgConverterInfo _elemConverterInfo;
    readonly int _rank;

    public MultiDimArrayConverterResolver(PgConverterInfo elemConverterInfo, int rank)
    {
        _elemConverterInfo = elemConverterInfo;
        _rank = rank;
    }

    public override PgConverterResolution<T> GetDefault(PgTypeId pgTypeId)
    {
        throw new NotImplementedException();
    }

    public override PgConverterResolution<T> Get(T? value, PgTypeId? expectedPgTypeId)
    {
        throw new NotImplementedException();
    }
}

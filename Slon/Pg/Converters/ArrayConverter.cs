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
    readonly Type _elementType;
    readonly int _pgLowerBound;

    public ArrayConverter(IArrayElementOperations elementOperations, bool elemTypeDbNullable, Type elementType, PgTypeId elemTypeId, ArrayPool<(ValueSize, object?)> statePool, int pgLowerBound = 1)
    {
        ElemTypeId = elemTypeId;
        _statePool = statePool;
        _elemTypeDbNullable = elemTypeDbNullable;
        _elementType = elementType;
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
            var context = new SizeContext(format, bufferLength);
            var sizeResult =
                elemTypeNullable && _elementOperations.IsDbNullValue(values, i)
                    ? ValueSize.Zero
                    : _elementOperations.GetSize(ref context, values, i);

            elemItem = (sizeResult, context.WriteState);
            // Set it to zero on an unknown/null byte count.
            bufferLength -= sizeResult.Value ?? bufferLength;
            totalSize = totalSize.Combine(sizeResult);
        }
        return totalSize;
    }

    ValueSize GetFixedElemsSize(Array values, DataFormat format)
    {
        var context = new SizeContext(format, 0);
        var fixedSize = _elementOperations.GetSize(ref context, values, 0).Value;
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

    public ValueSize GetSize(ref SizeContext context, Array values)
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
            context.WriteState = Array.Empty<(ValueSize, object?)>();
        }
        else
        {
            var stateArray = _statePool.Rent(values.Length);
            elemsSize = GetElemsSize(values, context.BufferLength - formatSize.Value ?? 0, stateArray, context.Format);
            context.WriteState = stateArray;
        }

        return formatSize.Combine(elemsSize);
    }

    public async ValueTask<Array> ReadCore(bool async, PgReader reader, int expectedDimensions, CancellationToken cancellationToken = default)
    {
        var dimensions = reader.ReadInt32();
        var containsNulls = reader.ReadInt32() == 1;
        reader.ReadUInt32(); // Element OID. Ignored.

        var returnType =
            // readAsObject
            // ? ArrayNullabilityMode switch
            // {
            //     ArrayNullabilityMode.Never => IsNonNullable && containsNulls
            //         ? throw new InvalidOperationException(ReadNonNullableCollectionWithNullsExceptionMessage)
            //         : ElementType,
            //     ArrayNullabilityMode.Always => nullableElementType,
            //     ArrayNullabilityMode.PerInstance => containsNulls
            //         ? nullableElementType
            //         : ElementType,
            //     _ => throw new ArgumentOutOfRangeException()
            // }
            // :
            _elemTypeDbNullable && containsNulls
                ? throw new InvalidOperationException()
                : _elementType;

        if (dimensions == 0)
            return expectedDimensions > 1
                ? Array.CreateInstance(returnType, new int[expectedDimensions])
                : _elementOperations.CreateArray(0);

        if (dimensions == 1 && returnType == _elementType)
        {
            var arrayLength = reader.ReadInt32();

            reader.ReadInt32(); // Lower bound

            var oneDimensional = _elementOperations.CreateArray(arrayLength);
            for (var i = 0; i < arrayLength; i++)
            {
                reader.ByteCount = reader.ReadInt32();
                await _elementOperations.Read(async, reader, oneDimensional, i, cancellationToken);
            }
            return oneDimensional;
        }

        throw new NotSupportedException();
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
            var context = new SizeContext(writer.Format, 0);
            var length = _elementOperations.GetSize(ref context, values, 0).Value.GetValueOrDefault();
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

            return elementOps.Write(async, writer, values, index, cancellationToken);
        }
    }
}

interface IArrayElementOperations
{
    Array CreateArray(int capacity);
    bool HasFixedSize(DataFormat format);
    ValueSize GetSize(ref SizeContext context, Array array, int index);
    bool IsDbNullValue(Array array, int index);
    ValueTask Read(bool async, PgReader reader, Array array, int index, CancellationToken cancellationToken = default);
    ValueTask Write(bool async, PgWriter writer, Array array, int index, CancellationToken cancellationToken = default);
}

sealed class ArrayConverter<T> : PgStreamingConverter<T?[]>, IArrayElementOperations
{
    readonly PgConverter<T> _elemConverter;
    readonly ArrayConverter _arrayConverter;

    public ArrayConverter(PgConverterResolution<T> resolution, ArrayPool<(ValueSize, object?)> statePool, int pgLowerBound = 1)
    {
        _elemConverter = resolution.Converter;
        _arrayConverter = new ArrayConverter(this, _elemConverter.IsDbNullable, typeof(T), resolution.PgTypeId, statePool, pgLowerBound);
    }

    internal PgTypeId ElemTypeId => _arrayConverter.ElemTypeId;

    public override bool CanConvert(DataFormat format) => _elemConverter.CanConvert(format);

    public override T?[] Read(PgReader reader) => (T?[])_arrayConverter.ReadCore(async: false, reader, 1).Result;

    public override async ValueTask<T?[]?> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => (T?[])await _arrayConverter.ReadCore(async: false, reader, 1, cancellationToken);

    public override ValueSize GetSize(ref SizeContext context, T?[] values)
        => _arrayConverter.GetSize(ref context, values);

    public override void Write(PgWriter writer, T?[] values)
        => _arrayConverter.WriteCore(async: false, writer, values, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask WriteAsync(PgWriter writer, T?[] values, CancellationToken cancellationToken = default)
        => _arrayConverter.WriteCore(async: true, writer, values, cancellationToken);

    public Array CreateArray(int capacity) => Array.Empty<T>();

    bool IArrayElementOperations.HasFixedSize(DataFormat format)
        => _elemConverter.HasFixedSize(format);

    ValueSize IArrayElementOperations.GetSize(ref SizeContext context, Array array, int index)
        => _elemConverter.GetSize(ref context, Unsafe.As<T?[]>(array)[index]!);

    bool IArrayElementOperations.IsDbNullValue(Array array, int index)
        => _elemConverter.IsDbNullValue(Unsafe.As<T?[]>(array)[index]);

    ValueTask IArrayElementOperations.Read(bool async, PgReader reader, Array array, int index, CancellationToken cancellationToken)
    {
        if (async)
        {
            var task = _elemConverter.ReadAsync(reader, cancellationToken);
            if (task.IsCompletedSuccessfully)
                Unsafe.As<T?[]>(array)[index] = task.GetAwaiter().GetResult();
            return Core(array, index, task);
        }

        Unsafe.As<T?[]>(array)[index] = _elemConverter.Read(reader);
        return new();

        static async ValueTask Core(Array array, int index, ValueTask<T?> task) => Unsafe.As<T?[]>(array)[index] = await task;
    }

    ValueTask IArrayElementOperations.Write(bool async, PgWriter writer, Array array, int index, CancellationToken cancellationToken)
    {
        if (async)
            return _elemConverter.WriteAsync(writer, Unsafe.As<T?[]>(array)[index]!, cancellationToken);

        _elemConverter.Write(writer, Unsafe.As<T?[]>(array)[index]!);
        return new();
    }
}

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

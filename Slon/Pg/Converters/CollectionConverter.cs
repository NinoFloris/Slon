using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg.Converters;

readonly struct PgArrayConverter
{
    readonly IElementOperations _elementOperations;
    public PgTypeId ElemTypeId { get; }
    readonly ArrayPool<(ValueSize, object?)> _statePool;
    readonly bool _elemTypeDbNullable;
    readonly Type _elementType;
    readonly int _pgLowerBound;

    public PgArrayConverter(IElementOperations elementOperations, bool elemTypeDbNullable, Type elementType, PgTypeId elemTypeId, ArrayPool<(ValueSize, object?)> statePool, int pgLowerBound = 1)
    {
        ElemTypeId = elemTypeId;
        _statePool = statePool;
        _elemTypeDbNullable = elemTypeDbNullable;
        _elementType = elementType;
        _pgLowerBound = pgLowerBound;
        _elementOperations = elementOperations;
    }

    ValueSize GetElemsSize(object values, int count, int bufferLength, (ValueSize, object?)[] elementStates, DataFormat format)
    {
        Debug.Assert(elementStates.Length == count);
        var totalSize = ValueSize.Zero;
        var elemTypeNullable = _elemTypeDbNullable;
        for (var i = 0; i < count; i++)
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

    ValueSize GetFixedElemsSize(object values, int count, DataFormat format)
    {
        var context = new SizeContext(format, 0);
        var fixedSize = _elementOperations.GetSize(ref context, values, 0).Value;
        var nonNullValues = count;
        if (_elemTypeDbNullable)
        {
            var nulls = 0;
            for (var i = 0; i < count; i++)
            {
                if (_elementOperations.IsDbNullValue(values, i))
                    nulls++;
            }

            nonNullValues -= nulls;
        }

        return ValueSize.Create(nonNullValues * fixedSize.GetValueOrDefault());
    }

    public ValueSize GetSize(ref SizeContext context, object values)
    {
        var count = _elementOperations.GetCollectionCount(values);
        var formatSize = ValueSize.Create(
            4 + // Dimensions
            4 + // Flags
            4 + // Element OID
            1 * 8 + // Dimensions * (array length and lower bound)
            4 * count // Element length integers
        );

        if (count == 0)
            return formatSize;

        ValueSize elemsSize;
        if (_elementOperations.HasFixedSize(context.Format))
        {
            elemsSize = GetFixedElemsSize(values, count, context.Format);
            context.WriteState = Array.Empty<(ValueSize, object?)>();
        }
        else
        {
            var stateArray = _statePool.Rent(count);
            elemsSize = GetElemsSize(values, count, context.BufferLength - formatSize.Value ?? 0, stateArray, context.Format);
            context.WriteState = stateArray;
        }

        return formatSize.Combine(elemsSize);
    }

    public async ValueTask<object> Read(bool async, PgReader reader, int expectedDimensions, CancellationToken cancellationToken = default)
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
                : _elementOperations.CreateCollection(0);

        if (dimensions == 1 && returnType == _elementType)
        {
            var arrayLength = reader.ReadInt32();

            reader.ReadInt32(); // Lower bound

            var oneDimensional = _elementOperations.CreateCollection(arrayLength);
            for (var i = 0; i < arrayLength; i++)
            {
                reader.ByteCount = reader.ReadInt32();
                await _elementOperations.Read(async, reader, oneDimensional, i, cancellationToken);
            }
            return oneDimensional;
        }

        throw new NotSupportedException();
    }

    public async ValueTask Write(bool async, PgWriter writer, object values, CancellationToken cancellationToken)
    {
        if (writer.State is not (ValueSize, object?)[] state)
            throw new InvalidOperationException($"Invalid state, expected {typeof((ValueSize, object?)[]).FullName}.");

        var count = _elementOperations.GetCollectionCount(values);
        writer.WriteInt32(1); // Dimensions
        writer.WriteInt32(0); // Flags (not really used)
        writer.WriteAsOid(ElemTypeId);
        writer.WriteInt32(count);
        writer.WriteInt32(_pgLowerBound);

        if (count is 0)
            return;

        var elemTypeDbNullable = _elemTypeDbNullable;
        var lastState = writer.State;

        // Fixed size path, we don't store anything.
        if (state.Length is 0)
        {
            var context = new SizeContext(writer.Format, 0);
            var length = _elementOperations.GetSize(ref context, values, 0).Value.GetValueOrDefault();
            for (var i = 0; i < count; i++)
            {
                if (elemTypeDbNullable && _elementOperations.IsDbNullValue(values, i))
                    writer.WriteInt32(-1);
                else
                    await WriteValue(_elementOperations, i, length, null).ConfigureAwait(false);
            }
        }
        else
            for (var i = 0; i < count && i < state.Length; i++)
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

        ValueTask WriteValue(IElementOperations elementOps, int index, int length, object? state)
        {
            writer.WriteInt32(length);

            if (state is not null || lastState is not null)
                writer.UpdateState(lastState = state, ValueSize.Create(length));

            return elementOps.Write(async, writer, values, index, cancellationToken);
        }
    }
}

interface IElementOperations
{
    object CreateCollection(int capacity);
    int GetCollectionCount(object collection);
    bool HasFixedSize(DataFormat format);
    ValueSize GetSize(ref SizeContext context, object collection, int index);
    bool IsDbNullValue(object collection, int index);
    ValueTask Read(bool async, PgReader reader, object collection, int index, CancellationToken cancellationToken = default);
    ValueTask Write(bool async, PgWriter writer, object collection, int index, CancellationToken cancellationToken = default);
}

interface ISetResult
{
    void Invoke(Task task, object collection, int index);
}

static class CollectionConverter
{
    // Split out from the generic class to amortize the huge size penalty per async state machine, which would otherwise be per instantiation.
#if !NETSTANDARD2_0
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
    public static async ValueTask AwaitReadTask(ISetResult setResult, Task task, object collection, int index)
    {
        await task;
        setResult.Invoke(task, collection, index);
    }
}

abstract class CollectionConverter<T> : PgStreamingConverter<T> where T : class
{
    readonly PgArrayConverter _pgArrayConverter;

    private protected CollectionConverter(PgConverterResolution elemResolution, ArrayPool<(ValueSize, object?)> statePool, int pgLowerBound = 1)
    {
        _pgArrayConverter = new PgArrayConverter((IElementOperations)this, elemResolution.Converter.IsDbNullable, elemResolution.Converter.TypeToConvert, elemResolution.PgTypeId, statePool, pgLowerBound);
    }

    internal PgTypeId ElemTypeId => _pgArrayConverter.ElemTypeId;

    public abstract override bool CanConvert(DataFormat format, out bool fixedSize);

    public override T Read(PgReader reader) => (T)_pgArrayConverter.Read(async: false, reader, 1).Result;

    public override Task<T> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => Unsafe.As<Task<T>>(_pgArrayConverter.Read(async: true, reader, 1, cancellationToken));

    public override ValueSize GetSize(ref SizeContext context, T values)
        => _pgArrayConverter.GetSize(ref context, values);

    public override void Write(PgWriter writer, T values)
        => _pgArrayConverter.Write(async: false, writer, values, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask WriteAsync(PgWriter writer, T values, CancellationToken cancellationToken = default)
        => _pgArrayConverter.Write(async: true, writer, values, cancellationToken);

}

sealed class ArrayConverter<TElement> : CollectionConverter<TElement?[]>, IElementOperations, ISetResult
{
    readonly PgConverter<TElement> _elemConverter;

    public ArrayConverter(PgConverterResolution<TElement> elemResolution, ArrayPool<(ValueSize, object?)> statePool, int pgLowerBound = 1)
        : base(elemResolution.ToConverterResolution(), statePool, pgLowerBound)
    {
        _elemConverter = elemResolution.Converter;
    }

    // We only support binary arrays for now.
    public override bool CanConvert(DataFormat format, out bool fixedSize)
    {
        fixedSize = false;
        return format is DataFormat.Binary && _elemConverter.CanConvert(format, out fixedSize);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static TElement? GetValue(object collection, int index)
    {
        Debug.Assert(collection is TElement?[]);
        return Unsafe.As<TElement?[]>(collection)[index];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void SetValue(object collection, int index, TElement? value)
    {
        Debug.Assert(collection is TElement?[]);
        Unsafe.As<TElement?[]>(collection)[index] = value;
    }

    object IElementOperations.CreateCollection(int capacity)
        => capacity is 0 ? Array.Empty<TElement?>() : new TElement?[capacity];

    int IElementOperations.GetCollectionCount(object collection)
    {
        Debug.Assert(collection is TElement?[]);
        return Unsafe.As<TElement?[]>(collection).Length;
    }

    bool IElementOperations.HasFixedSize(DataFormat format)
        => _elemConverter.CanConvert(format, out var fixedSize) && fixedSize;

    ValueSize IElementOperations.GetSize(ref SizeContext context, object collection, int index)
        => _elemConverter.GetSize(ref context, GetValue(collection, index)!);

    bool IElementOperations.IsDbNullValue(object collection, int index)
        => _elemConverter.IsDbNullValue(GetValue(collection, index));

    ValueTask IElementOperations.Read(bool async, PgReader reader, object collection, int index, CancellationToken cancellationToken)
    {
        TElement? result;
        if (!async)
            result = _elemConverter.Read(reader);
        else
        {
            var task = _elemConverter.ReadAsync(reader, cancellationToken);
            if (task.Status is not TaskStatus.RanToCompletion)
                return CollectionConverter.AwaitReadTask(this, task, collection, index);

            result = task.Result;
        }

        SetValue(collection, index, result);
        return new();
    }

    ValueTask IElementOperations.Write(bool async, PgWriter writer, object collection, int index, CancellationToken cancellationToken)
    {
        if (async)
            return _elemConverter.WriteAsync(writer, GetValue(collection, index)!, cancellationToken);

        _elemConverter.Write(writer, GetValue(collection, index)!);
        return new();
    }

    // Using valuetask to get the task result which is equivalent to GetAwaiter().GetResult for ValueTask, this removes TaskAwaiter<TElement> rooting.
    void ISetResult.Invoke(Task task, object collection, int index)
        => SetValue(collection, index, new ValueTask<TElement>((Task<TElement>)task).Result);
}
//
// sealed class ListConverter<TElement> : CollectionConverter<TElement, List<TElement?>>, IElementOperations
// {
//     public ListConverter(PgConverterResolution<TElement> elemResolution, ArrayPool<(ValueSize, object?)> statePool, int pgLowerBound = 1) : base(elemResolution, statePool, pgLowerBound)
//     {
//     }
//
//     TElement? GetValue(object collection, int index)
//     {
//         Debug.Assert(collection is List<TElement?>);
//         return Unsafe.As<List<TElement>>(collection)[index];
//     }
//
//     void SetValue(object collection, int index, TElement? value)
//     {
//         Debug.Assert(collection is List<TElement?>);
//         Unsafe.As<List<TElement?>>(collection)[index] = value;
//     }
//
//     object IElementOperations.CreateCollection(int capacity) => new List<TElement?>(capacity);
//
//     int IElementOperations.GetCollectionCount(object collection)
//     {
//         Debug.Assert(collection is List<TElement?>);
//         return Unsafe.As<List<TElement?>>(collection).Count;
//     }
//
//     bool IElementOperations.HasFixedSize(DataFormat format)
//         => ElemConverter.HasFixedSize(format);
//
//     ValueSize IElementOperations.GetSize(ref SizeContext context, object collection, int index)
//         => ElemConverter.GetSize(ref context, GetValue(collection, index)!);
//
//     bool IElementOperations.IsDbNullValue(object collection, int index)
//         => ElemConverter.IsDbNullValue(GetValue(collection, index));
//
//     ValueTask IElementOperations.Read(bool async, PgReader reader, object collection, int index, CancellationToken cancellationToken)
//     {
//         if (async)
//         {
//             var task = ElemConverter.ReadAsync(reader, cancellationToken);
//             if (task.IsCompletedSuccessfully)
//                 SetValue(collection, index, task.GetAwaiter().GetResult());
//             return Core(collection, index, task);
//         }
//
//         SetValue(collection, index, ElemConverter.Read(reader));
//         return new();
//
//         async ValueTask Core(object collection, int index, ValueTask<TElement?> task) => SetValue(collection, index, await task);
//     }
//
//     ValueTask IElementOperations.Write(bool async, PgWriter writer, object collection, int index, CancellationToken cancellationToken)
//     {
//         if (async)
//             return ElemConverter.WriteAsync(writer, GetValue(collection, index)!, cancellationToken);
//
//         ElemConverter.Write(writer, GetValue(collection, index)!);
//         return new();
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
    T? GetValueOrDefault(T?[]? values) => values is { } v ? v[0] : default;

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

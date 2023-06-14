using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Types;

namespace Slon.Pg.Converters;

interface IElementOperations
{
    object CreateCollection(int capacity, bool containsNulls);
    int GetCollectionCount(object collection);

    bool HasFixedSize(DataFormat format);
    ValueSize GetSize(ref SizeContext context, object collection, int index);
    bool IsDbNullValue(object collection, int index);
    ValueTask Read(bool async, PgReader reader, object collection, int index, CancellationToken cancellationToken = default);
    ValueTask Write(bool async, PgWriter writer, object collection, int index, CancellationToken cancellationToken = default);
}

readonly struct PgArrayConverter
{
    readonly IElementOperations _elementOperations;
    public bool ElemTypeDbNullable { get; }
    readonly ArrayPool<(ValueSize, object?)> _statePool;
    readonly int _pgLowerBound;
    readonly PgTypeId _elemTypeId;

    public PgArrayConverter(IElementOperations elementOperations, bool elemTypeDbNullable, PgTypeId elemTypeId, ArrayPool<(ValueSize, object?)> statePool, int pgLowerBound = 1)
    {
        _elemTypeId = elemTypeId;
        _statePool = statePool;
        ElemTypeDbNullable = elemTypeDbNullable;
        _pgLowerBound = pgLowerBound;
        _elementOperations = elementOperations;
    }

    ValueSize GetElemsSize(object values, int count, int bufferLength, (ValueSize, object?)[] elementStates, DataFormat format)
    {
        Debug.Assert(elementStates.Length == count);
        var totalSize = ValueSize.Zero;
        var elemTypeNullable = ElemTypeDbNullable;
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
        if (ElemTypeDbNullable)
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

    public async ValueTask<object> Read(bool async, PgReader reader, CancellationToken cancellationToken = default)
    {
        const int expectedDimensions = 1;

        var dimensions = reader.ReadInt32();
        var containsNulls = reader.ReadInt32() == 1;
        if (dimensions == 0)
            return _elementOperations.CreateCollection(0, containsNulls);

        if (dimensions != expectedDimensions)
            throw new InvalidOperationException($"Cannot read an array with {expectedDimensions} dimension(s) from an array with {dimensions} dimension(s)");

        reader.ReadUInt32(); // Element OID. Ignored.

        var arrayLength = reader.ReadInt32();

        reader.ReadInt32(); // Lower bound

        var collection = _elementOperations.CreateCollection(arrayLength, containsNulls);
        for (var i = 0; i < arrayLength; i++)
        {
            reader.ByteCount = reader.ReadInt32();
            await _elementOperations.Read(async, reader, collection, i, cancellationToken);
        }
        return collection;
    }

    public async ValueTask Write(bool async, PgWriter writer, object values, CancellationToken cancellationToken)
    {
        if (writer.State is not (ValueSize, object?)[] state)
            throw new InvalidOperationException($"Invalid state, expected {typeof((ValueSize, object?)[]).FullName}.");

        var count = _elementOperations.GetCollectionCount(values);
        writer.WriteInt32(1); // Dimensions
        writer.WriteInt32(0); // Flags (not really used)
        writer.WriteAsOid(_elemTypeId);
        writer.WriteInt32(count);
        writer.WriteInt32(_pgLowerBound);

        if (count is 0)
            return;

        var elemTypeDbNullable = ElemTypeDbNullable;
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

// Using a function pointer here is safe against assembly unloading as the instance reference that the static pointer method lives on is passed
// as such the instance cannot be collected by the gc which means the entire assembly is prevented from unloading until we're done.
// The alternatives are:
// 1. Add a virtual method to PgConverter and make AwaitReadTask call into it (bloating the vtable of all PgConverter derived types).
// 2. Using a delegate (a static field + an alloc per T + metadata, slightly slower dispatch perf, so strictly worse as well).
static class CollectionConverter
{
    // Split out from the generic class to amortize the huge size penalty per async state machine, which would otherwise be per instantiation.
#if !NETSTANDARD
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
    public static async ValueTask AwaitTask(Task task, Continuation continuation, object collection, int index)
    {
        await task;
        continuation.Invoke(task, collection, index);
        // Guarantee the type stays loaded until the function pointer call is done.
        GC.KeepAlive(continuation.Handle);
    }

    // Split out into a struct as unsafe and async don't mix, while we do want a nicely typed function pointer signature to prevent mistakes.
    public readonly unsafe struct Continuation
    {
        public object Handle { get; }
        readonly delegate*<Task, object, int, void> _continuation;

        /// <param name="handle">A reference to the type that houses the static method <see cref="continuation"/> points to.</param>
        /// <param name="continuation">The continuation</param>
        public Continuation(object handle, delegate*<Task, object, int, void> continuation)
        {
            Handle = handle;
            _continuation = continuation;
        }

        public void Invoke(Task task, object collection, int index) => _continuation(task, collection, index);
    }
}

abstract class ArrayConverter : PgStreamingConverter<object>
{
    readonly PgArrayConverter _pgArrayConverter;

    // TODO If we want to make the arrray converter public we probably want a strongly typed version.
    // public ArrayConverter(PgConverterResolution<TElement> elemResolution, ArrayPool<(ValueSize, object?)> statePool, int pgLowerBound = 1)
    //     : this(elemResolution.ToConverterResolution(), statePool, pgLowerBound) { }

    internal ArrayConverter(PgConverterResolution elemResolution, ArrayPool<(ValueSize, object?)> statePool, int pgLowerBound = 1)
    {
        _pgArrayConverter = new PgArrayConverter((IElementOperations)this, elemResolution.Converter.IsDbNullable, elemResolution.PgTypeId, statePool, pgLowerBound);
        if (!elemResolution.Converter.CanConvert(DataFormat.Binary, out _))
            throw new NotSupportedException("Element converter has to support the binary format to be compatible.");
    }

    public override object Read(PgReader reader) => _pgArrayConverter.Read(async: false, reader).Result;

    public override ValueTask<object> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => Unsafe.As<ValueTask<object>, ValueTask<object>>(ref Unsafe.AsRef(_pgArrayConverter.Read(async: true, reader, cancellationToken)));

    public override ValueSize GetSize(ref SizeContext context, object values)
        => _pgArrayConverter.GetSize(ref context, values);

    public override void Write(PgWriter writer, object values)
        => _pgArrayConverter.Write(async: false, writer, values, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask WriteAsync(PgWriter writer, object values, CancellationToken cancellationToken = default)
        => _pgArrayConverter.Write(async: true, writer, values, cancellationToken);

    protected void ThrowIfNullsNotSupported(bool containsNulls)
    {
        if (containsNulls && !_pgArrayConverter.ElemTypeDbNullable)
            throw new InvalidOperationException("Cannot read a non-nullable collection of elements because the returned array contains nulls. Call GetFieldValue with a nullable array type instead.");
    }
}

sealed class ArrayBasedArrayConverter<TElement> : ArrayConverter, IElementOperations
{
    readonly PgConverter<TElement> _elemConverter;

    public ArrayBasedArrayConverter(PgConverterResolution elemResolution, ArrayPool<(ValueSize, object?)>? statePool = null, int pgLowerBound = 1)
        : base(elemResolution, statePool ?? ArrayPool<(ValueSize, object?)>.Shared, pgLowerBound)
        => _elemConverter = (PgConverter<TElement>)elemResolution.Converter;

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

    object IElementOperations.CreateCollection(int capacity, bool containsNulls)
    {
        ThrowIfNullsNotSupported(containsNulls);
        return capacity is 0 ? Array.Empty<TElement?>() : new TElement?[capacity];
    }

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

    unsafe ValueTask IElementOperations.Read(bool async, PgReader reader, object collection, int index, CancellationToken cancellationToken)
    {
        TElement? result;
        if (!async)
            result = _elemConverter.Read(reader);
        else
        {
            var task = _elemConverter.ReadAsync(reader, cancellationToken);
            if (task.IsCompletedSuccessfully)
                return CollectionConverter.AwaitTask(task.AsTask(), new(this, &SetResult), collection, index);

            result = task.Result;
        }

        SetValue(collection, index, result);
        return new();

        // Using .Result on ValueTask is equivalent to GetAwaiter().GetResult(), this removes TaskAwaiter<TElement> rooting.
        static void SetResult(Task task, object collection, int index) => SetValue(collection, index, new ValueTask<TElement>((Task<TElement>)task).Result);
    }

    ValueTask IElementOperations.Write(bool async, PgWriter writer, object collection, int index, CancellationToken cancellationToken)
    {
        if (async)
            return _elemConverter.WriteAsync(writer, GetValue(collection, index)!, cancellationToken);

        _elemConverter.Write(writer, GetValue(collection, index)!);
        return new();
    }
}

sealed class ListBasedArrayConverter<TElement> : ArrayConverter, IElementOperations
{
    readonly PgConverter<TElement> _elemConverter;

    public ListBasedArrayConverter(PgConverterResolution elemResolution, ArrayPool<(ValueSize, object?)>? statePool = null, int pgLowerBound = 1)
        : base(elemResolution, statePool ?? ArrayPool<(ValueSize, object?)>.Shared, pgLowerBound)
        => _elemConverter = (PgConverter<TElement>)elemResolution.Converter;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static TElement? GetValue(object collection, int index)
    {
        Debug.Assert(collection is List<TElement?>);
        return Unsafe.As<List<TElement?>>(collection)[index];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void SetValue(object collection, int index, TElement? value)
    {
        Debug.Assert(collection is List<TElement?>);
        Unsafe.As<List<TElement?>>(collection)[index] = value;
    }

    object IElementOperations.CreateCollection(int capacity, bool containsNulls)
    {
        ThrowIfNullsNotSupported(containsNulls);
        return new List<TElement?>(capacity);
    }

    int IElementOperations.GetCollectionCount(object collection)
    {
        Debug.Assert(collection is List<TElement?>);
        return Unsafe.As<List<TElement?>>(collection).Count;
    }

    bool IElementOperations.HasFixedSize(DataFormat format)
        => _elemConverter.CanConvert(format, out var fixedSize) && fixedSize;

    ValueSize IElementOperations.GetSize(ref SizeContext context, object collection, int index)
        => _elemConverter.GetSize(ref context, GetValue(collection, index)!);

    bool IElementOperations.IsDbNullValue(object collection, int index)
        => _elemConverter.IsDbNullValue(GetValue(collection, index));

    unsafe ValueTask IElementOperations.Read(bool async, PgReader reader, object collection, int index, CancellationToken cancellationToken)
    {
        TElement? result;
        if (!async)
            result = _elemConverter.Read(reader);
        else
        {
            var task = _elemConverter.ReadAsync(reader, cancellationToken);
            if (task.IsCompletedSuccessfully)
                return CollectionConverter.AwaitTask(task.AsTask(), new(this, &SetResult), collection, index);

            result = task.Result;
        }

        SetValue(collection, index, result);
        return new();

        // Using .Result on ValueTask is equivalent to GetAwaiter().GetResult(), this removes TaskAwaiter<TElement> rooting.
        static void SetResult(Task task, object collection, int index) => SetValue(collection, index, new ValueTask<TElement>((Task<TElement>)task).Result);
    }

    ValueTask IElementOperations.Write(bool async, PgWriter writer, object collection, int index, CancellationToken cancellationToken)
    {
        if (async)
            return _elemConverter.WriteAsync(writer, GetValue(collection, index)!, cancellationToken);

        _elemConverter.Write(writer, GetValue(collection, index)!);
        return new();
    }
}

sealed class ArrayConverterResolver<TElement> : PgConverterResolver<object>
{
    readonly PgConverterInfo _elemConverterInfo;
    readonly ConcurrentDictionary<PgConverter<TElement>, ArrayBasedArrayConverter<TElement>> _arrayConverters = new(ReferenceEqualityComparer.Instance);
    readonly ConcurrentDictionary<PgConverter<TElement>, ListBasedArrayConverter<TElement>> _listConverters = new(ReferenceEqualityComparer.Instance);
    PgConverterResolution _lastElemResolution;
    PgConverterResolution _lastResolution;

    public ArrayConverterResolver(PgConverterInfo elemConverterInfo) => _elemConverterInfo = elemConverterInfo;

    public override PgConverterResolution GetDefault(PgTypeId pgTypeId) => Get(Array.Empty<TElement>(), pgTypeId);
    public override PgConverterResolution Get(object? values, PgTypeId? expectedPgTypeId)
    {
        // We get the pg type id for the first element to be able to pass it in for the subsequent, per element calls.
        // This is how we allow resolvers to catch value inconsistencies that would cause converter mixing and helps return useful error messages.
        // TODO we could remove this potential lookup by making resolvers intrinsically support checking for their array type ids.
        var elementTypeId = expectedPgTypeId is { } id ? (PgTypeId?)_elemConverterInfo.Options.GetElementTypeId(id) : null;
        var expectedResolution = _elemConverterInfo.GetResolution(GetFirstValueOrDefault(values), elementTypeId);

        ArrayConverter arrayConverter;
        switch (values)
        {
            case TElement[] vs:
            {
                foreach (var value in vs)
                    _ = _elemConverterInfo.GetResolution(value, expectedResolution.PgTypeId);

                if (ReferenceEquals(expectedResolution.Converter, _lastElemResolution.Converter) && expectedResolution.PgTypeId == _lastElemResolution.PgTypeId)
                    return _lastResolution;

                arrayConverter = GetOrAddArrayBased();

                break;
            }
            case List<TElement> vs:
            {
                foreach (var value in vs)
                    _ = _elemConverterInfo.GetResolution(value, expectedResolution.PgTypeId);

                if (ReferenceEquals(expectedResolution.Converter, _lastElemResolution.Converter) && expectedResolution.PgTypeId == _lastElemResolution.PgTypeId)
                    return _lastResolution;

                arrayConverter = GetOrAddListBased();

                break;
            }
            default:
                throw new NotSupportedException();
        }

        _lastElemResolution = expectedResolution;
        return _lastResolution = new PgConverterResolution(arrayConverter, expectedPgTypeId ?? _elemConverterInfo.Options.GetArrayTypeId(expectedResolution.PgTypeId));

        // We don't need to check lower bounds here, only relevant for multi dim.
        static TElement? GetFirstValueOrDefault(object? values) => values switch
        {
            TElement[] v => v[0],
            List<TElement> v => v[0],
            _ => default
        };

        ArrayBasedArrayConverter<TElement> GetOrAddArrayBased()
            => _arrayConverters.GetOrAdd(expectedResolution.GetConverter<TElement>(),
                static (elemConverter, expectedElemPgTypeId) =>
                    new ArrayBasedArrayConverter<TElement>(new(elemConverter, expectedElemPgTypeId)),
                expectedResolution.PgTypeId);

        ListBasedArrayConverter<TElement> GetOrAddListBased()
            => _listConverters.GetOrAdd(expectedResolution.GetConverter<TElement>(),
                static (elemConverter, expectedElemPgTypeId) =>
                    new ListBasedArrayConverter<TElement>(new(elemConverter, expectedElemPgTypeId)),
                expectedResolution.PgTypeId);
    }
}

sealed class PolymorphicCollectionConverter : PolymorphicReadConverter
{
    readonly PgConverter _structElementCollectionConverter;
    readonly PgConverter _nullableElementCollectionConverter;

    // EffectiveType is object[] as we only know per instance, after reading 'has nulls'.
    public PolymorphicCollectionConverter(PgConverter structElementCollectionConverter, PgConverter nullableElementCollectionConverter) : base(typeof(object[]))
    {
        _structElementCollectionConverter = structElementCollectionConverter;
        _nullableElementCollectionConverter = nullableElementCollectionConverter;
    }

    public override object Read(PgReader reader)
    {
        var remaining = reader.Remaining;
        var _ = reader.ReadInt32();
        var containsNulls = reader.ReadInt32() == 1;
        reader.Rewind(remaining - reader.Remaining);
        return (containsNulls ? _nullableElementCollectionConverter.ReadAsObject(reader) : _structElementCollectionConverter.ReadAsObject(reader))!;
    }

    public override ValueTask<object> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
    {
        var remaining = reader.Remaining;
        var _ = reader.ReadInt32();
        var containsNulls = reader.ReadInt32() == 1;
        reader.Rewind(remaining - reader.Remaining);
        return (containsNulls ? _nullableElementCollectionConverter.ReadAsObjectAsync(reader, cancellationToken) : _structElementCollectionConverter.ReadAsObjectAsync(reader, cancellationToken))!;
    }
}

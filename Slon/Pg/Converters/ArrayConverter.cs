using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg.Types;
using Slon.Protocol;

namespace Slon.Pg.Converters;

readonly struct ArrayConverter
{
    readonly IArrayElementOperations _elementOperations;
    readonly PgTypeId _elemTypeId;
    readonly ArrayPool<(SizeResult, object?)> _statePool;
    readonly bool _elemTypeDbNullable;
    readonly int _pgLowerBound;

    public ArrayConverter(IArrayElementOperations elementOperations, bool elemTypeDbNullable, PgTypeId elemTypeId, ArrayPool<(SizeResult, object?)> statePool, int pgLowerBound = 1)
    {
        _elemTypeId = elemTypeId;
        _statePool = statePool;
        _elemTypeDbNullable = elemTypeDbNullable;
        _pgLowerBound = pgLowerBound;
        _elementOperations = elementOperations;
    }

    SizeResult GetElementsSize(Array values, int bufferLength, (SizeResult, object?)[] elementStates, PgConverterOptions options)
    {
        Debug.Assert(elementStates.Length == values.Length);
        var totalSize = SizeResult.Zero;
        var elemTypeNullable = _elemTypeDbNullable;
        for (var i = 0; i < values.Length; i++)
        {
            ref var elemItem = ref elementStates[i];
            var elemState = (object?)null;
            var sizeResult =
                elemTypeNullable && _elementOperations.IsDbNullValue(values, i, options)
                ? SizeResult.Zero
                : _elementOperations.GetSize(values, i, bufferLength, ref elemState, options);

            if (sizeResult.Kind is SizeResultKind.FixedSize)
                throw new InvalidOperationException($"GetSize returning a fixed size {nameof(SizeResult)} should not decide to do so based on the passed in value, it should be static. Use a normal {nameof(SizeResult)} if that is not possible.");

            elemItem = (sizeResult, elemState);
            // Set it to zero on an unknown/null byte count.
            bufferLength -= sizeResult.Value ?? bufferLength;
            totalSize = totalSize.Combine(sizeResult);
        }
        return totalSize;
    }

    int? GetFixedSize(Array values, PgConverterOptions options)
    {
        var firstNonNullIndex = !_elemTypeDbNullable ? 0 : -1;
        if (_elemTypeDbNullable)
            for (var i = 0; i < values.Length; i++)
            {
                if (!_elementOperations.IsDbNullValue(values, i, options))
                {
                    firstNonNullIndex = i;
                    break;
                }
            }

        // We couldn't check any size as no element was acceptable as a non null value for GetSize, next time maybe.
        // This should rarely happen, effectively this means sending an entire array containing NULLs exclusively.
        if (firstNonNullIndex is -1)
            return null;

        var elemState = (object?)null;
        var sizeResult = _elementOperations.GetSize(values, firstNonNullIndex, 0, ref elemState, options);
        return sizeResult.Value is null || sizeResult.Kind is not SizeResultKind.FixedSize ? -1 : sizeResult.Value;
    }

    public SizeResult GetSize(Array values, int bufferLength, ref object? writeState, ref int? elemFixedSize, PgConverterOptions options)
    {
        var formatSize = SizeResult.Create(
            4 + // Dimensions
            4 + // Flags
            4 + // Element OID
            1 * 8 + // Dimensions * (array length and lower bound)
            4 * values.Length // Element length integers
        );

        if (values.Length == 0)
            return formatSize;

        var fixedSize = elemFixedSize ??= GetFixedSize(values, options);
        if (fixedSize is not null and not -1)
        {
            var nonNullValues = values.Length;
            if (_elemTypeDbNullable)
            {
                var nulls = 0;
                for (var i = 0; i < values.Length; i++)
                {
                    if (_elementOperations.IsDbNullValue(values, i, options))
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

    public async ValueTask WriteCore(bool async, int? elemFixedSize, PgWriter writer, Array values, PgConverterOptions options, CancellationToken cancellationToken)
    {
        if (writer.State is not (SizeResult, object?)[] state)
            state = writer.State is null && elemFixedSize is not null and not -1 ? null! :
                throw new InvalidOperationException($"Invalid state, expected {typeof((SizeResult, object?)[]).FullName}");

        writer.WriteInteger(1); // Dimensions
        writer.WriteInteger(0); // Flags (not really used)
        writer.WriteAsOid(_elemTypeId);
        writer.WriteInteger(values.Length);
        writer.WriteInteger(_pgLowerBound);

        var elemTypeDbNullable = _elemTypeDbNullable;
        var lastState = writer.State;

        // TODO reexamine null semantics, specifically structs with a null value.

        // The test for -1 happened at the start of the method.
        if (elemFixedSize is { } length)
            for (var i = 0; i < values.Length; i++)
            {
                if (elemTypeDbNullable && _elementOperations.IsDbNullValue(values, i, options))
                    writer.WriteInteger(-1);
                else
                    await WriteValue(_elementOperations, i, length, null).ConfigureAwait(false);
            }
        else
            for (var i = 0; i < values.Length && i < state.Length; i++)
            {
                if (elemTypeDbNullable && _elementOperations.IsDbNullValue(values, i, options))
                {
                    writer.WriteInteger(-1);
                    continue;
                }

                var (sizeResult, elemState) = state[i];
                switch (sizeResult.Kind)
                {
                    case SizeResultKind.Size:
                        await WriteValue(_elementOperations, i, sizeResult.Value.GetValueOrDefault(), elemState).ConfigureAwait(false);
                        break;
                    case SizeResultKind.UpperBound:
                        throw new NotImplementedException(); // TODO
                    case SizeResultKind.Unknown:
                        throw new NotImplementedException();
                        // {
                        //     using var bufferedOutput = options.GetBufferedOutput(elemConverter!, value, elemState, DataRepresentation.Binary);
                        //     writer.WriteInteger(bufferedOutput.Length);
                        //     if (async)
                        //         await bufferedOutput.WriteAsync(writer, cancellationToken).ConfigureAwait(false);
                        //     else
                        //         bufferedOutput.Write(writer);
                        // }
                        // break;
                    case SizeResultKind.FixedSize:
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                await writer.Flush(async, cancellationToken).ConfigureAwait(false);
            }

        ValueTask WriteValue(IArrayElementOperations elementOps, int index, int length, object? state)
        {
            writer.WriteInteger(length);

            if (state is not null || lastState is not null)
                writer.UpdateState(lastState = state, SizeResult.Create(length));

            if (async)
                return elementOps.WriteAsync(writer, values, index, options, cancellationToken);

            elementOps.Write(writer, values, index, options);
            return new ValueTask();
        }
    }
}

interface IArrayElementOperations
{
    SizeResult GetSize(Array array, int index, int bufferLength, ref object? writeState, PgConverterOptions options);
    bool IsDbNullValue(Array array, int index, PgConverterOptions options);
    void Write(PgWriter writer, Array array, int index, PgConverterOptions options);
    ValueTask WriteAsync(PgWriter writer, Array array, int index, PgConverterOptions options, CancellationToken cancellationToken = default);
}

sealed class ArrayConverter<T> : PgConverter<T?[]>, IArrayElementOperations
{
    readonly PgConverter<T> _elemConverter;
    int? _elemFixedSize;
    readonly ArrayConverter _arrayConverter;

    public ArrayConverter(PgConverter<T> elemConverter, PgTypeId elemTypeId, ArrayPool<(SizeResult, object?)> statePool, int pgLowerBound = 1)
    {
        _elemConverter = elemConverter;
        _arrayConverter = new ArrayConverter(this, _elemConverter.IsTypeDbNullable, elemTypeId, statePool, pgLowerBound);
    }

    public override bool CanConvert => _elemConverter.CanConvert;

    public override ReadStatus Read(ref SequenceReader<byte> reader, int byteCount, out T?[] value, PgConverterOptions options)
    {
        throw new NotImplementedException();
    }

    public override SizeResult GetSize(T?[] values, int bufferLength, ref object? writeState, PgConverterOptions options)
        => _arrayConverter.GetSize(values, bufferLength, ref writeState, ref _elemFixedSize, options);

    public override void Write(PgWriter writer, T?[] values, PgConverterOptions options)
        => _arrayConverter.WriteCore(async: false, _elemFixedSize, writer, values, options, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask WriteAsync(PgWriter writer, T?[] values, PgConverterOptions options, CancellationToken cancellationToken = default)
        => _arrayConverter.WriteCore(async: true, _elemFixedSize, writer, values, options, cancellationToken);

    // TODO implement text representation then this can be _elemConverter.CanTextConvert;
    public override bool CanTextConvert => false;

    SizeResult IArrayElementOperations.GetSize(Array array, int index, int bufferLength, ref object? writeState, PgConverterOptions options)
        => _elemConverter.GetSize(Unsafe.As<Array, T?[]>(ref array)[index]!, bufferLength, ref writeState, options);

    bool IArrayElementOperations.IsDbNullValue(Array array, int index, PgConverterOptions options)
        => _elemConverter.IsDbNullValue(Unsafe.As<Array, T?[]>(ref array)[index], options);

    void IArrayElementOperations.Write(PgWriter writer, Array array, int index, PgConverterOptions options)
        => _elemConverter.Write(writer, Unsafe.As<Array, T?[]>(ref array)[index]!, options);

    ValueTask IArrayElementOperations.WriteAsync(PgWriter writer, Array array, int index, PgConverterOptions options, CancellationToken cancellationToken)
        => _elemConverter.WriteAsync(writer, Unsafe.As<Array, T?[]>(ref array)[index]!, options, cancellationToken);
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
    [RequiresUnreferencedCode("Reflection used for pg type conversions.")]
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

        [RequiresUnreferencedCode("Reflection used for pg type conversions.")]
        PgConverterInfo CreateInfoFromElementInfo(PgConverterInfo elementInfo, PgConverter converter)
        {
            PgConverterInfo info;
            if (elementInfo is PgConverterResolverInfo)
                info = (PgConverterInfo)Activator.CreateInstance(typeof(PgConverterResolverInfo<>).MakeGenericType(type), options, converter)!;
            else
                info = (PgConverterInfo)Activator.CreateInstance(typeof(PgConverterInfo<>).MakeGenericType(type), options, converter, options.GetArrayTypeId(elementInfo.PgTypeId!.Value))!;

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

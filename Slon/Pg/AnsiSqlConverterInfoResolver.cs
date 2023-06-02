using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using Slon.Pg.Converters;
using Slon.Pg.Types;

namespace Slon.Pg;

/// <summary>
/// 
/// </summary>
/// <param name="options"></param>
/// <param name="mapping"></param>
/// <param name="resolvedDataTypeName">Signals whether a resolver based ConverterInfo can keep its PgTypeId undecided or whether it should follow mapping.DataTypeName.</param>
delegate PgConverterInfo ConverterInfoFactory(PgConverterOptions options, ConverterInfoMapping mapping, bool resolvedDataTypeName);

readonly struct ConverterInfoMapping
{
    public ConverterInfoMapping(Type type, DataTypeName dataTypeName, bool isDefault, ConverterInfoFactory factory)
    {
        Type = type;
        DataTypeName = dataTypeName;
        IsDefault = isDefault;
        Factory = factory;
    }

    public ConverterInfoFactory Factory { get; }
    public Type Type { get; }
    public DataTypeName DataTypeName { get; }
    public bool IsDefault { get; }

    public PgConverterInfo? GetConverterInfo(Type? type, DataTypeName? dataTypeName, PgConverterOptions options)
    {
        var dataTypeMatch = dataTypeName == DataTypeName;
        var typeMatch = type == Type;

        if (dataTypeMatch && typeMatch)
            return Factory(options, this, resolvedDataTypeName: true);

        // We only match as a default if the respective data wasn't passed.
        if (IsDefault && (dataTypeMatch && type is null || typeMatch && dataTypeName is null))
            return Factory(options, this, resolvedDataTypeName: dataTypeMatch);

        // Not a match
        return null;
    }
}

readonly struct ConverterInfoMappingCollection
{
    readonly List<ConverterInfoMapping> _items = new();

    public ConverterInfoMappingCollection()
    {
    }

    public IReadOnlyCollection<ConverterInfoMapping> Items => _items;

    ConverterInfoMapping? TryFindMapping(Type type, DataTypeName dataTypeName)
    {
        ConverterInfoMapping? elementMapping = null;
        foreach (var mapping in _items)
            if (mapping.Type == type && mapping.DataTypeName == dataTypeName)
            {
                elementMapping = mapping;
                break;
            }

        return elementMapping;
    }

    ConverterInfoMapping FindMapping(Type type, DataTypeName dataTypeName)
    {
        if (TryFindMapping(type, dataTypeName) is not { } mapping)
            throw new InvalidOperationException("Could not find mappings for " + dataTypeName);

        return mapping;
    }

    // Helper to eliminate generic display class duplication.
    static ConverterInfoFactory CreateComposedFactory(ConverterInfoMapping innerMapping, Func<PgConverterInfo, PgConverter> mapper, bool copyPreferredFormat = false) =>
        (options, mapping, resolvedDataTypeName) =>
        {
            var innerInfo = innerMapping.Factory(options, innerMapping, resolvedDataTypeName);
            if (innerMapping.IsDefault)
                return PgConverterInfo.CreateDefault(options, mapper(innerInfo), mapping.DataTypeName, copyPreferredFormat ? innerInfo.PreferredFormat : null);

            return PgConverterInfo.Create(options, mapper(innerInfo), mapping.DataTypeName, copyPreferredFormat ? innerInfo.PreferredFormat : null);
        };

    // Helper to eliminate generic display class duplication.
    static ConverterInfoFactory CreateComposedFactory(ConverterInfoMapping innerMapping, Func<PgConverterInfo, PgConverterResolver> mapper, bool copyPreferredFormat = false) =>
        (options, mapping, resolvedDataTypeName) =>
        {
            var innerInfo = innerMapping.Factory(options, innerMapping, resolvedDataTypeName);
            if (innerMapping.IsDefault)
                return PgConverterInfo.CreateDefault(options, mapper(innerInfo), resolvedDataTypeName ? mapping.DataTypeName : null, copyPreferredFormat ? innerInfo.PreferredFormat : null);

            return PgConverterInfo.Create(options, mapper(innerInfo), mapping.DataTypeName, copyPreferredFormat ? innerInfo.PreferredFormat : null);
        };

    void AddArrayType(ConverterInfoMapping elementMapping, Type type, Func<PgConverterInfo, PgConverter> converter)
    {
        var arrayDataTypeName = elementMapping.DataTypeName.ToArrayName();
        _items.Add(new ConverterInfoMapping(type, arrayDataTypeName, elementMapping.IsDefault, CreateComposedFactory(elementMapping, converter)));
    }

    public void AddType<T>(DataTypeName dataTypeName, ConverterInfoFactory createInfo, bool isDefault = false) where T : class
    {
        _items.Add(new ConverterInfoMapping(typeof(T), dataTypeName, isDefault, createInfo));
    }

    public void AddArrayType<TElement>(DataTypeName elementDataTypeName) where TElement: class
        => AddArrayType<TElement>(FindMapping(typeof(TElement), elementDataTypeName));

    public void AddArrayType<TElement>(ConverterInfoMapping elementMapping) where TElement : class
        => AddArrayType(elementMapping, typeof(TElement[]),
            static innerInfo => new ArrayConverter<TElement>(innerInfo.GetResolutionAsObject(), innerInfo.Options.GetArrayPool<(ValueSize, object?)>()));

    void AddStructType(Type type, Type nullableType, DataTypeName dataTypeName, ConverterInfoFactory createInfo, Func<PgConverterInfo, PgConverter> nullableConverter, bool isDefault)
    {
        ConverterInfoMapping mapping;
        _items.Add(mapping = new ConverterInfoMapping(type, dataTypeName, isDefault, createInfo));
        _items.Add(new ConverterInfoMapping(nullableType, dataTypeName, isDefault, CreateComposedFactory(mapping, nullableConverter, copyPreferredFormat: true)));
    }

    public void AddStructType<T>(DataTypeName dataTypeName, ConverterInfoFactory createInfo, bool isDefault = false) where T : struct
        => AddStructType(typeof(T), typeof(T?), dataTypeName, createInfo,
            static innerInfo => new NullableValueConverter<T>((PgBufferedConverter<T>)innerInfo.GetResolutionAsObject().Converter), isDefault);

    public void AddStreamingStructType<T>(DataTypeName dataTypeName, ConverterInfoFactory createInfo, bool isDefault = false) where T : struct
        => AddStructType(typeof(T), typeof(T?), dataTypeName, createInfo,
            static innerInfo => new StreamingNullableValueConverter<T>((PgStreamingConverter<T>)innerInfo.GetResolutionAsObject().Converter), isDefault);

    public void AddStructArrayType<TElement>(DataTypeName elementDataTypeName) where TElement: struct
        => AddStructArrayType<TElement>(FindMapping(typeof(TElement), elementDataTypeName), FindMapping(typeof(TElement?), elementDataTypeName));

    public void AddStructArrayType<TElement>(ConverterInfoMapping elementMapping, ConverterInfoMapping nullableElementMapping) where TElement : struct
        => AddStructArrayType(elementMapping, nullableElementMapping, typeof(TElement[]), typeof(TElement?[]),
            static elemInfo => new ArrayConverter<TElement>(elemInfo.GetResolutionAsObject(), elemInfo.Options.GetArrayPool<(ValueSize, object?)>()),
            static elemInfo => new ArrayConverter<TElement?>(elemInfo.GetResolutionAsObject(), elemInfo.Options.GetArrayPool<(ValueSize, object?)>()));

    void AddStructArrayType(ConverterInfoMapping elementMapping, ConverterInfoMapping nullableElementMapping, Type type, Type nullableType, Func<PgConverterInfo, PgConverter> converter, Func<PgConverterInfo, PgConverter> nullableConverter)
    {
        var arrayDataTypeName = elementMapping.DataTypeName.ToArrayName();
        ConverterInfoMapping arrayMapping;
        ConverterInfoMapping nullableArrayMapping;
        _items.Add(arrayMapping = new ConverterInfoMapping(type, arrayDataTypeName, elementMapping.IsDefault, CreateComposedFactory(elementMapping, converter)));
        _items.Add(nullableArrayMapping = new ConverterInfoMapping(nullableType, arrayDataTypeName, isDefault: false, CreateComposedFactory(nullableElementMapping, nullableConverter)));
        _items.Add(new ConverterInfoMapping(typeof(object), arrayDataTypeName, isDefault: false, (options, mapping, resolvedDataTypeName) => options.ArrayNullabilityMode switch
        {
            ArrayNullabilityMode.Never => arrayMapping.Factory(options, arrayMapping, resolvedDataTypeName).ToObjectConverterInfo(isDefault: false),
            ArrayNullabilityMode.Always => nullableArrayMapping.Factory(options, nullableArrayMapping, resolvedDataTypeName).ToObjectConverterInfo(isDefault: false),
            ArrayNullabilityMode.PerInstance => arrayMapping.Factory(options, arrayMapping, resolvedDataTypeName).ToComposedConverterInfo(
                new PolymorphicCollectionConverter(
                    arrayMapping.Factory(options, arrayMapping, resolvedDataTypeName).GetResolutionAsObject().Converter,
                    nullableArrayMapping.Factory(options, nullableArrayMapping, resolvedDataTypeName).GetResolutionAsObject().Converter
                ), mapping.DataTypeName, isDefault: false),
            _ => throw new ArgumentOutOfRangeException()
        }));
    }

    public void AddResolverStructArrayType<TElement>(DataTypeName elementDataTypeName) where TElement: struct
        => AddResolverStructArrayType<TElement>(FindMapping(typeof(TElement), elementDataTypeName), FindMapping(typeof(TElement?), elementDataTypeName));

    public void AddResolverStructArrayType<TElement>(ConverterInfoMapping elementMapping, ConverterInfoMapping nullableElementMapping) where TElement : struct
        => AddResolverStructArrayType(elementMapping, nullableElementMapping, typeof(TElement[]), typeof(TElement?[]),
            static elemInfo => new ArrayConverterResolver<TElement>(elemInfo),
            static elemInfo => new ArrayConverterResolver<TElement?>(elemInfo));

    void AddResolverStructArrayType(ConverterInfoMapping elementMapping, ConverterInfoMapping nullableElementMapping, Type type, Type nullableType, Func<PgConverterInfo, PgConverterResolver> converter, Func<PgConverterInfo, PgConverterResolver> nullableConverter)
    {
        var arrayDataTypeName = elementMapping.DataTypeName.ToArrayName();
        ConverterInfoMapping arrayMapping;
        ConverterInfoMapping nullableArrayMapping;
        _items.Add(arrayMapping = new ConverterInfoMapping(type, arrayDataTypeName, elementMapping.IsDefault, CreateComposedFactory(elementMapping, converter)));
        _items.Add(nullableArrayMapping = new ConverterInfoMapping(nullableType, arrayDataTypeName, isDefault: false, CreateComposedFactory(nullableElementMapping, nullableConverter)));
        _items.Add(new ConverterInfoMapping(typeof(object), arrayDataTypeName, isDefault: false, (options, mapping, resolvedDataTypeName) => options.ArrayNullabilityMode switch
        {
            ArrayNullabilityMode.Never => arrayMapping.Factory(options, arrayMapping, resolvedDataTypeName).ToObjectConverterInfo(isDefault: false),
            ArrayNullabilityMode.Always => nullableArrayMapping.Factory(options, nullableArrayMapping, resolvedDataTypeName).ToObjectConverterInfo(isDefault: false),
            ArrayNullabilityMode.PerInstance => arrayMapping.Factory(options, arrayMapping, resolvedDataTypeName).ToComposedConverterInfo(
                new PolymorphicCollectionConverter(
                    arrayMapping.Factory(options, arrayMapping, resolvedDataTypeName).GetResolutionAsObject().Converter,
                    nullableArrayMapping.Factory(options, nullableArrayMapping, resolvedDataTypeName).GetResolutionAsObject().Converter
                ), mapping.DataTypeName, isDefault: false),
            _ => throw new ArgumentOutOfRangeException()
        }));
    }
}

static class PgConverterInfoHelpers
{
    public static PgConverterInfo CreateInfo(this ConverterInfoMapping mapping, PgConverterOptions options, PgConverterResolver resolver, bool resolvedDataTypeName = true, DataFormat? preferredFormat = null)
    {
        if (mapping.IsDefault)
            return PgConverterInfo.CreateDefault(options, resolver, resolvedDataTypeName ? mapping.DataTypeName : null, preferredFormat);

        return PgConverterInfo.Create(options, resolver, mapping.DataTypeName, preferredFormat);
    }

    public static PgConverterInfo CreateInfo(this ConverterInfoMapping mapping, PgConverterOptions options, PgConverter converter, DataFormat? preferredFormat = null)
    {
        if (mapping.IsDefault)
            return PgConverterInfo.CreateDefault(options, converter, mapping.DataTypeName, preferredFormat);

        return PgConverterInfo.Create(options, converter, mapping.DataTypeName, preferredFormat);
    }
}

class AnsiSqlConverterInfoResolver: IPgConverterInfoResolver
{
    static readonly ConverterInfoMapping[] _mappings;

    static void AddInfos(ConverterInfoMappingCollection mappings)
    {
        // Bool
        mappings.AddStructType<bool>(DataTypeNames.Bool,
            static (options, mapping, _) => mapping.CreateInfo(options, new BoolConverter()), isDefault: true);

        // Int2
        mappings.AddStructType<short>(DataTypeNames.Int2,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int16Converter<short>()), isDefault: true);
        mappings.AddStructType<int>(DataTypeNames.Int2,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int16Converter<int>()));
        mappings.AddStructType<long>(DataTypeNames.Int2,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int16Converter<long>()));
        mappings.AddStructType<byte>(DataTypeNames.Int2,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int16Converter<byte>()));
        mappings.AddStructType<sbyte>(DataTypeNames.Int2,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int16Converter<sbyte>()));

        // Int4
        mappings.AddStructType<short>(DataTypeNames.Int4,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int32Converter<short>()));
        mappings.AddStructType<int>(DataTypeNames.Int4,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int32Converter<int>()), isDefault: true);
        mappings.AddStructType<long>(DataTypeNames.Int4,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int32Converter<long>()));
        mappings.AddStructType<byte>(DataTypeNames.Int4,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int32Converter<byte>()));
        mappings.AddStructType<sbyte>(DataTypeNames.Int4,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int32Converter<sbyte>()));

        // Int8
        mappings.AddStructType<short>(DataTypeNames.Int8,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int64Converter<short>()));
        mappings.AddStructType<int>(DataTypeNames.Int8,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int64Converter<int>()));
        mappings.AddStructType<long>(DataTypeNames.Int8,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int64Converter<long>()), isDefault: true);
        mappings.AddStructType<byte>(DataTypeNames.Int8,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int64Converter<byte>()));
        mappings.AddStructType<sbyte>(DataTypeNames.Int8,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int64Converter<sbyte>()));

        // Float4
        mappings.AddStructType<float>(DataTypeNames.Float4,
            static (options, mapping, _) => mapping.CreateInfo(options, new RealConverter<float>()));
        mappings.AddStructType<double>(DataTypeNames.Float4,
            static (options, mapping, _) => mapping.CreateInfo(options, new RealConverter<double>()));

        // Float8
        mappings.AddStructType<double>(DataTypeNames.Float8,
            static (options, mapping, _) => mapping.CreateInfo(options, new DoubleConverter<double>()));

        // Numeric
        mappings.AddStructType<BigInteger>(DataTypeNames.Numeric,
            static (options, mapping, _) => mapping.CreateInfo(options, new NumericConverter<BigInteger>()));
        mappings.AddStructType<decimal>(DataTypeNames.Numeric,
            static (options, mapping, _) => mapping.CreateInfo(options, new NumericConverter<decimal>()), isDefault: true);
        mappings.AddStructType<short>(DataTypeNames.Numeric,
            static (options, mapping, _) => mapping.CreateInfo(options, new NumericConverter<short>()));
        mappings.AddStructType<int>(DataTypeNames.Numeric,
            static (options, mapping, _) => mapping.CreateInfo(options, new NumericConverter<int>()));
        mappings.AddStructType<long>(DataTypeNames.Numeric,
            static (options, mapping, _) => mapping.CreateInfo(options, new NumericConverter<long>()));
        mappings.AddStructType<float>(DataTypeNames.Numeric,
            static (options, mapping, _) => mapping.CreateInfo(options, new NumericConverter<float>()));
        mappings.AddStructType<double>(DataTypeNames.Numeric,
            static (options, mapping, _) => mapping.CreateInfo(options, new NumericConverter<double>()));

        // TODO might want to move to pg specific types.
        // Varbit
        mappings.AddType<BitArray>(DataTypeNames.Varbit,
            static (options, mapping, _) => mapping.CreateInfo(options, new BitArrayBitStringConverter(options.GetArrayPool<byte>())), isDefault: true);
        mappings.AddStructType<bool>(DataTypeNames.Varbit,
            static (options, mapping, _) => mapping.CreateInfo(options, new BoolBitStringConverter()));
        mappings.AddStructType<BitVector32>(DataTypeNames.Varbit,
            static (options, mapping, _) => mapping.CreateInfo(options, new BitVector32BitStringConverter()));
        mappings.AddType<object>(DataTypeNames.Varbit,
            static (options, mapping, _) => mapping.CreateInfo(options,
                new PolymorphicBitStringConverterResolver(options.GetCanonicalTypeId(DataTypeNames.Varbit))));

        // Bit
        mappings.AddType<BitArray>(DataTypeNames.Bit,
            static (options, mapping, _) => mapping.CreateInfo(options, new BitArrayBitStringConverter(options.GetArrayPool<byte>())), isDefault: true);
        mappings.AddStructType<bool>(DataTypeNames.Bit,
            static (options, mapping, _) => mapping.CreateInfo(options, new BoolBitStringConverter()));
        mappings.AddStructType<BitVector32>(DataTypeNames.Bit,
            static (options, mapping, _) => mapping.CreateInfo(options, new BitVector32BitStringConverter()));
        mappings.AddType<object>(DataTypeNames.Bit,
            static (options, mapping, _) => mapping.CreateInfo(options,
                new PolymorphicBitStringConverterResolver(options.GetCanonicalTypeId(DataTypeNames.Bit))));

        // TimestampTz
        mappings.AddStructType<DateTime>(DataTypeNames.TimestampTz,
            static (options, mapping, resolvedDataTypeName) => mapping.CreateInfo(options,
                new DateTimeConverterResolver(options.GetCanonicalTypeId(DataTypeNames.TimestampTz), options.GetCanonicalTypeId(DataTypeNames.Timestamp),
                    options.EnableDateTimeInfinityConversions), resolvedDataTypeName), isDefault: true);
        mappings.AddStructType<DateTimeOffset>(DataTypeNames.TimestampTz,
            static (options, mapping, _) => mapping.CreateInfo(options,
                new DateTimeOffsetUtcOnlyConverterResolver(options.GetCanonicalTypeId(DataTypeNames.TimestampTz),
                    options.EnableDateTimeInfinityConversions)));
        mappings.AddStructType<long>(DataTypeNames.TimestampTz,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int64Converter<long>()));

        // Timestamp
        mappings.AddStructType<DateTime>(DataTypeNames.Timestamp,
            static (options, mapping, resolvedDataTypeName) => mapping.CreateInfo(options,
                new DateTimeConverterResolver(options.GetCanonicalTypeId(DataTypeNames.TimestampTz), options.GetCanonicalTypeId(DataTypeNames.Timestamp),
                    options.EnableDateTimeInfinityConversions), resolvedDataTypeName), isDefault: true);
        mappings.AddStructType<long>(DataTypeNames.Timestamp,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int64Converter<long>()));

        // Time
        mappings.AddStructType<TimeSpan>(DataTypeNames.Time,
            static (options, mapping, _) => mapping.CreateInfo(options, new TimeSpanTimeConverter()), isDefault: true);
        mappings.AddStructType<long>(DataTypeNames.Time,
            static (options, mapping, _) => mapping.CreateInfo(options, new Int64Converter<long>()));
#if NET6_0_OR_GREATER
        mappings.AddStructType<TimeOnly>(DataTypeNames.Time,
            static (options, mapping, _) => mapping.CreateInfo(options, new TimeOnlyTimeConverter()));
#endif

        // TimeTz
        mappings.AddStructType<DateTimeOffset>(DataTypeNames.TimeTz,
            static (options, mapping, _) => mapping.CreateInfo(options, new DateTimeOffsetTimeTzConverter()), isDefault: true);

        // Interval
        mappings.AddStructType<TimeSpan>(DataTypeNames.Interval,
            static (options, mapping, _) => mapping.CreateInfo(options, new TimeSpanIntervalConverter()), isDefault: true);

        // Text
        mappings.AddType<string>(DataTypeNames.Text,
            static (options, mapping, _) => mapping.CreateInfo(options, new StringTextConverter(new ReadOnlyMemoryTextConverter(options), options), DataFormat.Text), isDefault: true);
        mappings.AddType<char[]>(DataTypeNames.Text,
            static (options, mapping, _) => mapping.CreateInfo(options, new CharArrayTextConverter(new ReadOnlyMemoryTextConverter(options)), DataFormat.Text));
        mappings.AddStreamingStructType<ReadOnlyMemory<char>>(DataTypeNames.Text,
            static (options, mapping, _) => mapping.CreateInfo(options, new ReadOnlyMemoryTextConverter(options), DataFormat.Text));
        mappings.AddStreamingStructType<ArraySegment<char>>(DataTypeNames.Text,
            static (options, mapping, _) => mapping.CreateInfo(options, new CharArraySegmentTextConverter(new ReadOnlyMemoryTextConverter(options)), DataFormat.Text));
        mappings.AddStructType<char>(DataTypeNames.Text,
            static (options, mapping, _) => mapping.CreateInfo(options, new CharTextConverter(options), DataFormat.Text));
    }

    static void AddArrayInfos(ConverterInfoMappingCollection mappings)
    {
        // Bool
        mappings.AddStructArrayType<bool>(DataTypeNames.Bool);

        // Int2
        mappings.AddStructArrayType<short>(DataTypeNames.Int2);
        mappings.AddStructArrayType<int>(DataTypeNames.Int2);
        mappings.AddStructArrayType<long>(DataTypeNames.Int2);
        mappings.AddStructArrayType<byte>(DataTypeNames.Int2);
        mappings.AddStructArrayType<sbyte>(DataTypeNames.Int2);

        // Int4
        mappings.AddStructArrayType<short>(DataTypeNames.Int4);
        mappings.AddStructArrayType<int>(DataTypeNames.Int4);
        mappings.AddStructArrayType<long>(DataTypeNames.Int4);
        mappings.AddStructArrayType<byte>(DataTypeNames.Int4);
        mappings.AddStructArrayType<sbyte>(DataTypeNames.Int4);

        // Int8
        mappings.AddStructArrayType<short>(DataTypeNames.Int8);
        mappings.AddStructArrayType<int>(DataTypeNames.Int8);
        mappings.AddStructArrayType<long>(DataTypeNames.Int8);
        mappings.AddStructArrayType<byte>(DataTypeNames.Int8);
        mappings.AddStructArrayType<sbyte>(DataTypeNames.Int8);

        // Float4
        mappings.AddStructArrayType<float>(DataTypeNames.Float4);
        mappings.AddStructArrayType<double>(DataTypeNames.Float4);

        // Float8
        mappings.AddStructArrayType<double>(DataTypeNames.Float8);

        // Numeric
        mappings.AddStructArrayType<BigInteger>(DataTypeNames.Numeric);
        mappings.AddStructArrayType<decimal>(DataTypeNames.Numeric);
        mappings.AddStructArrayType<short>(DataTypeNames.Numeric);
        mappings.AddStructArrayType<int>(DataTypeNames.Numeric);
        mappings.AddStructArrayType<long>(DataTypeNames.Numeric);
        mappings.AddStructArrayType<float>(DataTypeNames.Numeric);
        mappings.AddStructArrayType<double>(DataTypeNames.Numeric);

        // Varbit
        mappings.AddArrayType<BitArray>(DataTypeNames.Varbit);
        mappings.AddStructArrayType<bool>(DataTypeNames.Varbit);
        mappings.AddStructArrayType<BitVector32>(DataTypeNames.Varbit);
        mappings.AddArrayType<object>(DataTypeNames.Varbit);

        // Bit
        mappings.AddArrayType<BitArray>(DataTypeNames.Bit);
        mappings.AddStructArrayType<bool>(DataTypeNames.Bit);
        mappings.AddStructArrayType<BitVector32>(DataTypeNames.Bit);
        mappings.AddArrayType<object>(DataTypeNames.Bit);

        // TimestampTz
        mappings.AddResolverStructArrayType<DateTime>(DataTypeNames.TimestampTz);
        mappings.AddStructArrayType<DateTimeOffset>(DataTypeNames.TimestampTz);
        mappings.AddStructArrayType<long>(DataTypeNames.TimestampTz);

        // Timestamp
        mappings.AddResolverStructArrayType<DateTime>(DataTypeNames.Timestamp);
        mappings.AddStructArrayType<long>(DataTypeNames.Timestamp);

        // Time
        mappings.AddStructArrayType<TimeSpan>(DataTypeNames.Time);
        mappings.AddStructArrayType<long>(DataTypeNames.Time);
#if NET6_0_OR_GREATER
        mappings.AddStructArrayType<TimeOnly>(DataTypeNames.Time);
#endif

        // TimeTz
        mappings.AddStructArrayType<DateTimeOffset>(DataTypeNames.TimeTz);

        // Interval
        mappings.AddStructArrayType<TimeSpan>(DataTypeNames.Interval);

        // Text
        mappings.AddArrayType<string>(DataTypeNames.Text);
        mappings.AddArrayType<char[]>(DataTypeNames.Text);
        mappings.AddStructArrayType<ReadOnlyMemory<char>>(DataTypeNames.Text);
        mappings.AddStructArrayType<ArraySegment<char>>(DataTypeNames.Text);
        mappings.AddStructArrayType<char>(DataTypeNames.Text);
    }

    static AnsiSqlConverterInfoResolver()
    {
        var mappings = new ConverterInfoMappingCollection();
        AddInfos(mappings);
        var elementTypeCount = mappings.Items.Count;
        AddArrayInfos(mappings);
        Debug.Assert(elementTypeCount * 2 == mappings.Items.Count);
        _mappings = mappings.Items.ToArray();
    }

    // Default mappings.
    static (Type? defaultType, DataTypeName? defaultName) TryGetDefault(Type? type, DataTypeName? dataTypeName) =>
        (type, dataTypeName) switch
        {
            (null, null) => throw new InvalidOperationException($"At miminum one non-null {nameof(type)} or {nameof(dataTypeName)} is required."),
            _ when type == typeof(float) || dataTypeName == DataTypeNames.Float4 => (typeof(float), DataTypeNames.Float4),
            _ when type == typeof(double) || dataTypeName == DataTypeNames.Float8 => (typeof(double), DataTypeNames.Float8),
            _ when type == typeof(decimal) || dataTypeName == DataTypeNames.Numeric => (typeof(decimal), DataTypeNames.Numeric),
            // The typed default is important to get all the DataTypeName values lifted into a nullable. Don't simplify to default.
            // Moving the types to the destructure also won't work because a default value is allowed to assign to a nullable of that type.
            _ => default((Type?, DataTypeName?))
        };

    public PgConverterInfo? GetConverterInfo(Type? type, DataTypeName? dataTypeName, PgConverterOptions options)
    {
        foreach (var mapping in _mappings)
        {
            if (mapping.GetConverterInfo(type, dataTypeName, options) is { } info)
                return info;
        }

        return null;
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using Slon.Pg.Converters;
using Slon.Pg.Types;

namespace Slon.Pg;

readonly struct ConverterInfoMapping
{
    public ConverterInfoMapping(Type type, DataTypeName dataTypeName, bool isDefault, Func<ConverterInfoMapping, PgConverterOptions, PgConverterInfo> factory)
    {
        Type = type;
        DataTypeName = dataTypeName;
        IsDefault = isDefault;
        Factory = factory;
    }

    public Func<ConverterInfoMapping, PgConverterOptions, PgConverterInfo> Factory { get; }
    public Type Type { get; }
    public DataTypeName DataTypeName { get; }
    public bool IsDefault { get; }

    public PgConverterInfo? GetConverterInfo(Type? type, DataTypeName? dataTypeName, PgConverterOptions options)
    {
        if (type == Type && (IsDefault || dataTypeName == DataTypeName))
            return Factory(this, options);

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
    static Func<ConverterInfoMapping, PgConverterOptions, PgConverterInfo> CreateComposedFactory(ConverterInfoMapping innerMapping, Func<PgConverterInfo, PgConverter> mapper) =>
        (mapping, options) =>
        {
            var info = innerMapping.Factory(innerMapping, options);
            return info.ToComposedConverterInfo(mapper(info), mapping.DataTypeName, isDefault: mapping.IsDefault);
        };

    void AddArrayType(ConverterInfoMapping elementMapping, Type type, Func<PgConverterInfo, PgConverter> converter)
    {
        var arrayDataTypeName = elementMapping.DataTypeName.ToArrayName();
        _items.Add(new ConverterInfoMapping(type, arrayDataTypeName, elementMapping.IsDefault, CreateComposedFactory(elementMapping, converter)));
    }

    public void AddType<T>(DataTypeName dataTypeName, Func<ConverterInfoMapping, PgConverterOptions, PgConverterInfo> createInfo, bool isDefault = false) where T : class
    {
        _items.Add(new ConverterInfoMapping(typeof(T), dataTypeName, isDefault, createInfo));
    }

    public void AddArrayType<TElement>(DataTypeName elementDataTypeName) where TElement: class
        => AddArrayType<TElement>(FindMapping(typeof(TElement), elementDataTypeName));

    public void AddArrayType<TElement>(ConverterInfoMapping elementMapping) where TElement : class
        => AddArrayType(elementMapping, typeof(TElement[]), static info => new ArrayConverter<TElement>(info.GetResolutionAsObject(null), info.Options.GetArrayPool<(ValueSize, object?)>()));

    void AddStructType(Type type, Type nullableType, DataTypeName dataTypeName, Func<ConverterInfoMapping, PgConverterOptions, PgConverterInfo> createInfo, Func<PgConverterInfo, PgConverter> nullableConverter, bool isDefault)
    {
        ConverterInfoMapping mapping;
        _items.Add(mapping = new ConverterInfoMapping(type, dataTypeName, isDefault, createInfo));
        _items.Add(new ConverterInfoMapping(nullableType, dataTypeName, isDefault, CreateComposedFactory(mapping, nullableConverter)));
    }

    public void AddStructType<T>(DataTypeName dataTypeName, Func<ConverterInfoMapping, PgConverterOptions, PgConverterInfo> createInfo, bool isDefault = false) where T : struct
        => AddStructType(typeof(T), typeof(T?), dataTypeName, createInfo, static info => new NullableValueConverter<T>((PgBufferedConverter<T>)info.GetResolutionAsObject(null).Converter), isDefault);

    public void AddStreamingStructType<T>(DataTypeName dataTypeName, Func<ConverterInfoMapping, PgConverterOptions, PgConverterInfo> createInfo, bool isDefault = false) where T : struct
        => AddStructType(typeof(T), typeof(T?), dataTypeName, createInfo, static info => new StreamingNullableValueConverter<T>((PgStreamingConverter<T>)info.GetResolutionAsObject(null).Converter), isDefault);

    public void AddStructArrayType<TElement>(DataTypeName elementDataTypeName) where TElement: struct
        => AddStructArrayType<TElement>(FindMapping(typeof(TElement), elementDataTypeName), FindMapping(typeof(TElement?), elementDataTypeName));

    public void AddStructArrayType<TElement>(ConverterInfoMapping elementMapping, ConverterInfoMapping nullableElementMapping) where TElement : struct
        => AddStructArrayType(elementMapping, nullableElementMapping, typeof(TElement[]), typeof(TElement?[]),
            static info => new ArrayConverter<TElement>(info.GetResolutionAsObject(null), info.Options.GetArrayPool<(ValueSize, object?)>()),
            static info => new ArrayConverter<TElement?>(info.GetResolutionAsObject(null), info.Options.GetArrayPool<(ValueSize, object?)>()));

    void AddStructArrayType(ConverterInfoMapping elementMapping, ConverterInfoMapping nullableElementMapping, Type type, Type nullableType, Func<PgConverterInfo, PgConverter> converter, Func<PgConverterInfo, PgConverter> nullableConverter)
    {
        var arrayDataTypeName = elementMapping.DataTypeName.ToArrayName();
        ConverterInfoMapping arrayMapping;
        ConverterInfoMapping nullableArrayMapping;
        _items.Add(arrayMapping = new ConverterInfoMapping(type, arrayDataTypeName, elementMapping.IsDefault, CreateComposedFactory(elementMapping, converter)));
        _items.Add(nullableArrayMapping = new ConverterInfoMapping(nullableType, arrayDataTypeName, isDefault: false, CreateComposedFactory(nullableElementMapping, nullableConverter)));
        _items.Add(new ConverterInfoMapping(typeof(object), arrayDataTypeName, isDefault: false, (mapping, options) => options.ArrayNullabilityMode switch
        {
            ArrayNullabilityMode.Never => arrayMapping.Factory(mapping, options).ToObjectConverterInfo(isDefault: false),
            ArrayNullabilityMode.Always => nullableArrayMapping.Factory(mapping, options).ToObjectConverterInfo(isDefault: false),
            ArrayNullabilityMode.PerInstance => arrayMapping.Factory(mapping, options).ToComposedConverterInfo(
                new PolymorphicCollectionConverter(
                    arrayMapping.Factory(mapping, options).GetResolutionAsObject(null).Converter,
                    nullableArrayMapping.Factory(mapping, options).GetResolutionAsObject(null).Converter
                ), mapping.DataTypeName, isDefault: false),
            _ => throw new ArgumentOutOfRangeException()
        }));
    }
}

class AnsiSqlConverterInfoResolver: IPgConverterInfoResolver
{
    static readonly ConverterInfoMapping[] _mappings;

    static void AddInfos(ConverterInfoMappingCollection mappings)
    {
        // Bool
        mappings.AddStructType<bool>(DataTypeNames.Bool,
            (mapping, options) => PgConverterInfo.Create(options, new BoolConverter(), mapping.DataTypeName, mapping.IsDefault),
            isDefault: true);

        // Int2
        mappings.AddStructType<short>(DataTypeNames.Int2, CreateInt2ConverterInfo<short>, isDefault: true);
        mappings.AddStructType<int>(DataTypeNames.Int2, CreateInt2ConverterInfo<int>);
        mappings.AddStructType<long>(DataTypeNames.Int2, CreateInt2ConverterInfo<long>);
        mappings.AddStructType<byte>(DataTypeNames.Int2, CreateInt2ConverterInfo<byte>);
        mappings.AddStructType<sbyte>(DataTypeNames.Int2, CreateInt2ConverterInfo<sbyte>);

        // Int4
        mappings.AddStructType<short>(DataTypeNames.Int4, CreateInt4ConverterInfo<short>);
        mappings.AddStructType<int>(DataTypeNames.Int4, CreateInt4ConverterInfo<int>, isDefault: true);
        mappings.AddStructType<long>(DataTypeNames.Int4, CreateInt4ConverterInfo<long>);
        mappings.AddStructType<byte>(DataTypeNames.Int4, CreateInt4ConverterInfo<byte>);
        mappings.AddStructType<sbyte>(DataTypeNames.Int4, CreateInt4ConverterInfo<sbyte>);

        // Int8
        mappings.AddStructType<short>(DataTypeNames.Int8, CreateInt8ConverterInfo<short>);
        mappings.AddStructType<int>(DataTypeNames.Int8, CreateInt8ConverterInfo<int>);
        mappings.AddStructType<long>(DataTypeNames.Int8, CreateInt8ConverterInfo<long>, isDefault: true);
        mappings.AddStructType<byte>(DataTypeNames.Int8, CreateInt8ConverterInfo<byte>);
        mappings.AddStructType<sbyte>(DataTypeNames.Int8, CreateInt8ConverterInfo<sbyte>);

        // Text
        mappings.AddType<string>(DataTypeNames.Text, static (mapping, options) =>
            PgConverterInfo.Create(options, new StringTextConverter(new ReadOnlyMemoryTextConverter(options), options), mapping.DataTypeName, mapping.IsDefault, DataFormat.Text),
            isDefault: true);
        mappings.AddType<char[]>(DataTypeNames.Text, static (mapping, options) =>
            PgConverterInfo.Create(options, new CharArrayTextConverter(new ReadOnlyMemoryTextConverter(options)), mapping.DataTypeName, mapping.IsDefault, DataFormat.Text));
        mappings.AddStreamingStructType<ReadOnlyMemory<char>>(DataTypeNames.Text, static (mapping, options) =>
            PgConverterInfo.Create(options, new ReadOnlyMemoryTextConverter(options), mapping.DataTypeName, mapping.IsDefault, DataFormat.Text));
        mappings.AddStreamingStructType<ArraySegment<char>>(DataTypeNames.Text, static (mapping, options) =>
            PgConverterInfo.Create(options, new CharArraySegmentTextConverter(new ReadOnlyMemoryTextConverter(options)), mapping.DataTypeName, mapping.IsDefault, DataFormat.Text));
        mappings.AddStructType<char>(DataTypeNames.Text, static (mapping, options) =>
            PgConverterInfo.Create(options, new CharTextConverter(options), mapping.DataTypeName, mapping.IsDefault, DataFormat.Text));
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

    static PgConverterInfo CreateInt2ConverterInfo<T>(ConverterInfoMapping mapping, PgConverterOptions options)
#if !NETSTANDARD2_0
        where T : INumberBase<T>
#endif
        => PgConverterInfo.Create(options, new Int16Converter<T>(), mapping.DataTypeName, mapping.IsDefault);

    static PgConverterInfo CreateInt4ConverterInfo<T>(ConverterInfoMapping mapping, PgConverterOptions options)
#if !NETSTANDARD2_0
        where T : INumberBase<T>
#endif
        => PgConverterInfo.Create(options, new Int32Converter<T>(), mapping.DataTypeName, mapping.IsDefault);

    static PgConverterInfo CreateInt8ConverterInfo<T>(ConverterInfoMapping mapping, PgConverterOptions options)
#if !NETSTANDARD2_0
        where T : INumberBase<T>
#endif
        => PgConverterInfo.Create(options, new Int64Converter<T>(), mapping.DataTypeName, mapping.IsDefault);
}

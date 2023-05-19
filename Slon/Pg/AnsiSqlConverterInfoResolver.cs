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

    public void AddType<T>(DataTypeName baseDataTypeName, Func<ConverterInfoMapping, PgConverterOptions, PgConverterInfo> createInfo, bool isDefault = false)
    {
        _items.Add(new ConverterInfoMapping(typeof(T), baseDataTypeName, isDefault, createInfo));
    }

    public void AddArrayType<TElement>(DataTypeName elementDataTypeName)
    {
        AddArrayType<TElement>(_items.Single(x => x.Type == typeof(TElement) && x.DataTypeName == elementDataTypeName));
    }

    public void AddArrayType<TElement>(ConverterInfoMapping elementMapping)
    {
        var arrayDataTypeName = elementMapping.DataTypeName.ToArrayName();
        _items.Add(new ConverterInfoMapping(typeof(TElement[]), arrayDataTypeName, elementMapping.IsDefault, CreateArrayInfo));
        // _mappings.Add(new ConverterInfoMapping(typeof(List<TElement>), arrayDataTypeName, baseMapping.IsDefaultMapping, CreateListInfo));

        PgConverterInfo CreateArrayInfo(ConverterInfoMapping mapping, PgConverterOptions options)
        {
            var baseTypeInfo = elementMapping.Factory(mapping, options);
            return baseTypeInfo.IsValueDependent
                ? baseTypeInfo.Compose(new ArrayConverterResolver<TElement>(baseTypeInfo),
                    arrayDataTypeName)
                : baseTypeInfo.Compose(
                    new ArrayConverter<TElement>(baseTypeInfo.GetResolution<TElement>(default, elementMapping.DataTypeName),
                        baseTypeInfo.Options.GetArrayPool<(ValueSize, object?)>()),
                    baseTypeInfo.Options.GetArrayTypeId(baseTypeInfo.PgTypeId.GetValueOrDefault()));
        }

        // PgConverterInfo CreateListInfo(ConverterInfoMapping mapping, PgConverterOptions options)
        // {
        //     throw new NotImplementedException();
        // }
    }
}

class AnsiSqlConverterInfoResolver: IPgConverterInfoResolver
{
    static readonly ConverterInfoMapping[] _mappings;

    static void AddInfos(ConverterInfoMappingCollection mappings)
    {
        // Bool
        mappings.AddType<bool>(DataTypeNames.Bool,
            (mapping, options) => PgConverterInfo.Create(options, new BoolConverter(), mapping.DataTypeName, mapping.IsDefault),
            isDefault: true);

        // Int2
        mappings.AddType<short>(DataTypeNames.Int2, CreateInt2ConverterInfo<short>, isDefault: true);
        mappings.AddType<int>(DataTypeNames.Int2, CreateInt2ConverterInfo<int>);
        mappings.AddType<long>(DataTypeNames.Int2, CreateInt2ConverterInfo<long>);
        mappings.AddType<byte>(DataTypeNames.Int2, CreateInt2ConverterInfo<byte>);
        mappings.AddType<sbyte>(DataTypeNames.Int2, CreateInt2ConverterInfo<sbyte>);

        // Int4
        mappings.AddType<short>(DataTypeNames.Int4, CreateInt4ConverterInfo<short>);
        mappings.AddType<int>(DataTypeNames.Int4, CreateInt4ConverterInfo<int>, isDefault: true);
        mappings.AddType<long>(DataTypeNames.Int4, CreateInt4ConverterInfo<long>);
        mappings.AddType<byte>(DataTypeNames.Int4, CreateInt4ConverterInfo<byte>);
        mappings.AddType<sbyte>(DataTypeNames.Int4, CreateInt4ConverterInfo<sbyte>);

        // Int8
        mappings.AddType<short>(DataTypeNames.Int8, CreateInt8ConverterInfo<short>);
        mappings.AddType<int>(DataTypeNames.Int8, CreateInt8ConverterInfo<int>);
        mappings.AddType<long>(DataTypeNames.Int8, CreateInt8ConverterInfo<long>, isDefault: true);
        mappings.AddType<byte>(DataTypeNames.Int8, CreateInt8ConverterInfo<byte>);
        mappings.AddType<sbyte>(DataTypeNames.Int8, CreateInt8ConverterInfo<sbyte>);

        // Text
        mappings.AddType<string>(DataTypeNames.Text, static (mapping, options) =>
            PgConverterInfo.Create(options, new StringTextConverter(new ReadOnlyMemoryTextConverter(options), options), mapping.DataTypeName, mapping.IsDefault, DataFormat.Text),
            isDefault: true);
        mappings.AddType<char[]>(DataTypeNames.Text, static (mapping, options) =>
            PgConverterInfo.Create(options, new CharArrayTextConverter(new ReadOnlyMemoryTextConverter(options)), mapping.DataTypeName, mapping.IsDefault, DataFormat.Text));
        mappings.AddType<ReadOnlyMemory<char>>(DataTypeNames.Text, static (mapping, options) =>
            PgConverterInfo.Create(options, new ReadOnlyMemoryTextConverter(options), mapping.DataTypeName, mapping.IsDefault, DataFormat.Text));
        mappings.AddType<ArraySegment<char>>(DataTypeNames.Text, static (mapping, options) =>
            PgConverterInfo.Create(options, new CharArraySegmentTextConverter(new ReadOnlyMemoryTextConverter(options)), mapping.DataTypeName, mapping.IsDefault, DataFormat.Text));
        mappings.AddType<char>(DataTypeNames.Text, static (mapping, options) =>
            PgConverterInfo.Create(options, new CharTextConverter(options), mapping.DataTypeName, mapping.IsDefault, DataFormat.Text));
    }

    static void AddArrayInfos(ConverterInfoMappingCollection mappings)
    {
        // Bool
        mappings.AddArrayType<bool>(DataTypeNames.Bool);

        // Int2
        mappings.AddArrayType<short>(DataTypeNames.Int2);
        mappings.AddArrayType<int>(DataTypeNames.Int2);
        mappings.AddArrayType<long>(DataTypeNames.Int2);
        mappings.AddArrayType<byte>(DataTypeNames.Int2);
        mappings.AddArrayType<sbyte>(DataTypeNames.Int2);

        // Int4
        mappings.AddArrayType<short>(DataTypeNames.Int4);
        mappings.AddArrayType<int>(DataTypeNames.Int4);
        mappings.AddArrayType<long>(DataTypeNames.Int4);
        mappings.AddArrayType<byte>(DataTypeNames.Int4);
        mappings.AddArrayType<sbyte>(DataTypeNames.Int4);

        // Int8
        mappings.AddArrayType<short>(DataTypeNames.Int8);
        mappings.AddArrayType<int>(DataTypeNames.Int8);
        mappings.AddArrayType<long>(DataTypeNames.Int8);
        mappings.AddArrayType<byte>(DataTypeNames.Int8);
        mappings.AddArrayType<sbyte>(DataTypeNames.Int8);

        // Text
        mappings.AddArrayType<string>(DataTypeNames.Text);
        mappings.AddArrayType<char[]>(DataTypeNames.Text);
        mappings.AddArrayType<ReadOnlyMemory<char>>(DataTypeNames.Text);
        mappings.AddArrayType<ArraySegment<char>>(DataTypeNames.Text);
        mappings.AddArrayType<char>(DataTypeNames.Text);
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

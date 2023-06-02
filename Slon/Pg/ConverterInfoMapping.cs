using System;
using System.Collections.Generic;
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

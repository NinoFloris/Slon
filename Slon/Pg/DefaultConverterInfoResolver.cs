using System;
using System.Numerics;
using Slon.Pg.Converters;
using Slon.Pg.Types;

namespace Slon.Pg;

class DefaultConverterInfoResolver: IPgConverterInfoResolver
{
    static ReadOnlyMemoryTextConverter? _romTextConverter;

    public PgConverterInfo? GetConverterInfo(Type? type, DataTypeName? dataTypeName, PgConverterOptions options)
    {
        // Default mappings.
        var (defaultType, defaultName) = (type, dataTypeName) switch
        {
            (null, null) => throw new InvalidOperationException($"At miminum one non-null {nameof(type)} or {nameof(dataTypeName)} is required."),
            _ when type == typeof(int) || dataTypeName == DataTypeNames.Int4 => (typeof(int), DataTypeNames.Int4),
            _ when type == typeof(long) || dataTypeName == DataTypeNames.Int8 => (typeof(long), DataTypeNames.Int8),
            _ when type == typeof(short) || dataTypeName == DataTypeNames.Int2 => (typeof(short), DataTypeNames.Int2),
            _ when type == typeof(float) || dataTypeName == DataTypeNames.Float4 => (typeof(float), DataTypeNames.Float4),
            _ when type == typeof(double) || dataTypeName == DataTypeNames.Float8 => (typeof(double), DataTypeNames.Float8),
            _ when type == typeof(bool) || dataTypeName == DataTypeNames.Bool => (typeof(bool), DataTypeNames.Bool),
            _ when type == typeof(decimal) || dataTypeName == DataTypeNames.Numeric => (typeof(decimal), DataTypeNames.Numeric),
            _ when type == typeof(string) || dataTypeName == DataTypeNames.Text => (typeof(string), DataTypeNames.Text),

            // The typed default is important to get all the DataTypeName values lifted into a nullable. Don't simplify to default.
            // Moving the types to the destructure also won't work because a default value is allowed to assign to a nullable of that type.
            _ => default((Type?, DataTypeName?))
        };
        type ??= defaultType;
        dataTypeName ??= defaultName;
        // Either we could find defaults for a given DataTypeName *or* a clr type MUST have been passed for us to do anything.
        if (type is null)
            return null;
        // We want defaultness to be intrinsic to the mapping, not just a result of the absence of a clr type.
        // So (null, DataTypeName.Int4), (typeof(int), null), (typeof(int), DataTypeName.Int4) should all return a default info.
        var isDefaultInfo = dataTypeName is null ? type == defaultType : type == defaultType && dataTypeName == defaultName;

        // Number converters.
        // We're using dataTypeName.Value when there is a default mapping to make sure everything stays in sync (or throws).
        // If there is no default name for the clr type we have to provide one, when making a type default be sure to replace it here with .Value.
        // var numberInfo = type switch
        // {
        //     _ when type == typeof(int) => CreateNumberInfo<int>(dataTypeName!.Value),
        //     _ when type == typeof(long) => CreateNumberInfo<long>(dataTypeName!.Value),
        //     _ when type == typeof(short) => CreateNumberInfo<short>(dataTypeName!.Value),
        //     _ when type == typeof(float) => CreateNumberInfo<float>(dataTypeName!.Value),
        //     _ when type == typeof(double) => CreateNumberInfo<double>(dataTypeName!.Value),
        //     _ when type == typeof(decimal) => CreateNumberInfo<decimal>(dataTypeName!.Value),
        //     _ when type == typeof(byte) => CreateNumberInfo<byte>(dataTypeName ?? DataTypeNames.Int2),
        //     _ when type == typeof(sbyte) => CreateNumberInfo<sbyte>(dataTypeName ?? DataTypeNames.Int2),
        //     _ => null
        // };
        // if (numberInfo is not null)
        //     return numberInfo;

        // Text converters.
        var textInfo = type switch
        {
            _ when type == typeof(string) => CreateTextInfo(new StringTextConverter(_romTextConverter ??= new ReadOnlyMemoryTextConverter(options), options)),
            _ when type == typeof(char[]) => CreateTextInfo(new CharArrayTextConverter(_romTextConverter ??= new ReadOnlyMemoryTextConverter(options))),
            _ when type == typeof(ReadOnlyMemory<char>) => CreateTextInfo(_romTextConverter ??= new ReadOnlyMemoryTextConverter(options)),
            _ when type == typeof(ArraySegment<char>) => CreateTextInfo(new CharArraySegmentTextConverter(_romTextConverter ??= new ReadOnlyMemoryTextConverter(options))),
            _ when type == typeof(char) => CreateTextInfo(new CharTextConverter(options)),
            _ => null
        };
        if (textInfo is not null)
            return textInfo;

        return null;

        PgConverterInfo CreateTextInfo<T>(PgConverter<T> converter)
            => PgConverterInfo.Create(options, converter, DataTypeNames.Text, isDefaultInfo, DataFormat.Text);
//
//         PgConverterInfo? CreateNumberInfo<T>(DataTypeName dataTypeName)
// #if !NETSTANDARD2_0
//         where T : struct, INumberBase<T>
// # else
//             where T : struct
// #endif
//             => this.CreateNumberInfo<T>(dataTypeName, isDefaultInfo, options);
    }
//
//     PgConverterInfo? CreateNumberInfo<T>(DataTypeName dataTypeName, bool isDefaultInfo, PgConverterOptions options)
// #if !NETSTANDARD2_0
//         where T : struct, INumberBase<T>
// # else
//         where T : struct
// #endif
//     {
//         PgConverter<T>? converter = null;
//         switch (dataTypeName)
//         {
//             case var _ when dataTypeName == DataTypeNames.Int2:
// #if NETSTANDARD2_0
//                 if (TypeSupport.IsSupported(Int16Converter.SupportedTypes, typeof(T)))
// #endif
//                     converter = new Int16Converter<T>();
//                 break;
//             case var _ when dataTypeName == DataTypeNames.Int4:
// #if NETSTANDARD2_0
//                 if (TypeSupport.IsSupported(Int32Converter.SupportedTypes, typeof(T)))
// #endif
//                     converter = new Int32Converter<T>();
//                 break;
//             case var _ when dataTypeName == DataTypeNames.Int8:
// #if NETSTANDARD2_0
//                 if (TypeSupport.IsSupported(Int64Converter.SupportedTypes, typeof(T)))
// #endif
//                     converter = new Int64Converter<T>();
//                 break;
//             // TODO
//             // DataTypeNames.Float4
//             // DataTypeNames.Float8
//             // DataTypeNames.Numeric
//         }
//
//         return converter is not null ? PgConverterInfo.Create(options, converter, dataTypeName, isDefaultInfo) : null;
//     }
}

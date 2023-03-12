using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using Slon.Pg.Types;

namespace Slon.Pg;

class PgConverterOptions
{
    object? _converterInfoCache;

    /// Whether options should return a portable identifier (data type name) to prevent any generated id (oid) confusion across backends, this comes with a perf penalty.
    internal bool RequirePortableTypeIds => TypeCatalog.IsPortable;
    internal required PgTypeCatalog TypeCatalog { get; init; }

    public required Encoding TextEncoding { get; init; }
    public required IPgConverterInfoResolver ConverterInfoResolver { get; init; }
    public bool EnableDateTimeInfinityConversions { get; init; } = true;

    PgConverterInfo? GetConverterInfoCore(Type? type, PgTypeId? pgTypeId)
    {
        // We don't verify the kind of pgTypeId we get, it'll throw if it's incorrect.
        // It's up to the Converter author to call GetCanonicalTypeId if they want to use an oid instead of a datatypename.
        // Effectively it should be 'impossible' to get the wrong kind via any PgConverterOptions api which is what this is mainly for.
        if (RequirePortableTypeIds)
        {
            return Unsafe.As<ConverterInfoCache<DataTypeName>>(_converterInfoCache ??= new ConverterInfoCache<DataTypeName>(this))
                .GetOrAddInfo(type, pgTypeId is { } id ? id.DataTypeName : null);
        }
        else
        {
            return Unsafe.As<ConverterInfoCache<Oid>>(_converterInfoCache ??= new ConverterInfoCache<Oid>(this))
                .GetOrAddInfo(type, pgTypeId is { } id ? id.Oid : null);
        }
    }

    public PgConverterInfo? GetDefaultConverterInfo(PgTypeId pgTypeId)
        => GetConverterInfoCore(null, pgTypeId);

    public PgConverterInfo? GetConverterInfo(Type type, PgTypeId? pgTypeId = null)
        => GetConverterInfoCore(type ?? throw new ArgumentNullException(), pgTypeId);

    // If a given type id is in the opposite form than what was expected it will be mapped according to the requirement.
    internal PgTypeId GetCanonicalTypeId(PgTypeId pgTypeId)
        => RequirePortableTypeIds ? TypeCatalog.GetDataTypeName(pgTypeId) : TypeCatalog.GetOid(pgTypeId);

    public PgTypeId GetTypeId(string dataTypeName)
        => RequirePortableTypeIds ? TypeCatalog.GetDataTypeName(dataTypeName) : TypeCatalog.GetOid(TypeCatalog.GetDataTypeName(dataTypeName));

    public PgTypeId GetTypeId(DataTypeName dataTypeName)
        => RequirePortableTypeIds ? TypeCatalog.GetDataTypeName((PgTypeId)dataTypeName) : TypeCatalog.GetOid(dataTypeName);

    public PgTypeId GetArrayTypeId(string elementDataTypeName)
        => RequirePortableTypeIds
            ? TypeCatalog.GetArrayDataTypeName(TypeCatalog.GetDataTypeName(elementDataTypeName))
            : TypeCatalog.GetArrayOid(TypeCatalog.GetDataTypeName(elementDataTypeName));

    public PgTypeId GetArrayTypeId(DataTypeName elementDataTypeName)
        => RequirePortableTypeIds ? TypeCatalog.GetArrayDataTypeName(elementDataTypeName) : TypeCatalog.GetArrayOid(elementDataTypeName);

    public PgTypeId GetArrayTypeId(PgTypeId elementTypeId)
        => RequirePortableTypeIds ? TypeCatalog.GetArrayDataTypeName(elementTypeId) : TypeCatalog.GetArrayOid(elementTypeId);

    public PgTypeId GetElementTypeId(string arrayDataTypeName)
        => RequirePortableTypeIds
            ? TypeCatalog.GetElementDataTypeName(TypeCatalog.GetDataTypeName(arrayDataTypeName))
            : TypeCatalog.GetElementOid(TypeCatalog.GetDataTypeName(arrayDataTypeName));

    public PgTypeId GetElementTypeId(DataTypeName arrayDataTypeName)
        => RequirePortableTypeIds ? TypeCatalog.GetElementDataTypeName(arrayDataTypeName) : TypeCatalog.GetElementOid(arrayDataTypeName);

    public PgTypeId GetElementTypeId(PgTypeId arrayTypeId)
        => RequirePortableTypeIds ? TypeCatalog.GetElementDataTypeName(arrayTypeId) : TypeCatalog.GetElementOid(arrayTypeId);

    public PgWriter GetBufferedWriter<TWriter>(TWriter bufferWriter, object? state) where TWriter : IBufferWriter<byte>
    {
        var bufferedWriter = (PgWriter)null!;
        bufferedWriter.UpdateState(state, ValueSize.Unknown);
        return bufferedWriter;
    }

    public BufferedOutput GetBufferedOutput<T>(PgConverter<T> converter, T value, object? state, DataRepresentation dataRepresentation)
    {
        var writer = GetBufferedWriter<IBufferWriter<byte>>(null!, state); // TODO this should be some array pool thing.
        converter.Write(writer, value, this);

        return new BufferedOutput(default);
    }

    public ArrayPool<T> GetArrayPool<T>() => ArrayPool<T>.Shared;
}

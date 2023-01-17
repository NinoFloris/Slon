using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Npgsql.Pipelines.Pg.Types;

namespace Npgsql.Pipelines.Pg;

sealed class ConverterInfoCache<TPgTypeId> where TPgTypeId : struct
{
    readonly PgConverterOptions _options;

    // 8ns
    readonly ConcurrentDictionary<Type, PgConverterInfo> _cacheByClrType = new(); // most used for parameter writing
    // 8ns, about 10ns total to scan an array with 6, 7 different clr types under one pg type
    readonly ConcurrentDictionary<TPgTypeId, PgConverterInfo[]> _cacheByPgTypeId = new(); // Used for reading, occasionally for parameter writing where a db type was given.

    public ConverterInfoCache(PgConverterOptions options)
    {
        _options = options;

        if (typeof(TPgTypeId) != typeof(Oid) && typeof(TPgTypeId) != typeof(DataTypeName))
            throw new InvalidOperationException("Cannot use this type argument.");
    }

    PgTypeId? AsPgTypeId(TPgTypeId? pgTypeId)
    {
        if (pgTypeId is { } id)
            return _options.RequirePortableTypeIds switch
            {
                true => new PgTypeId(Unsafe.As<TPgTypeId, DataTypeName>(ref id)),
                false => new PgTypeId(Unsafe.As<TPgTypeId, Oid>(ref id))
            };

        return null;
    }

    public PgConverterInfo? GetOrAddInfo(Type? type, TPgTypeId? pgTypeId)
    {
        if (pgTypeId is null && type is not null)
        {
            // No GetOrAdd as we don't want to cache potential nulls.
            return _cacheByClrType.TryGetValue(type, out var info) ? info : AddByType(type);
        }

        if (pgTypeId is not { } id)
            return null;

        if (_cacheByPgTypeId.TryGetValue(id, out var infos))
            foreach (var cachedInfo in infos)
                if (type is null && cachedInfo.IsDefault || cachedInfo.Type == type)
                    return cachedInfo;

        return AddByIdEntry(id, infos);

        PgConverterInfo? AddByType(Type type)
        {
            var info = CreateInfo();
            if (info is null)
                return null;

            // We never remove entries so either of these branches will always succeed.
            return _cacheByClrType.TryAdd(type, info) ? info : _cacheByClrType[type];
        }

        PgConverterInfo? AddByIdEntry(TPgTypeId pgTypeId, PgConverterInfo[]? infos)
        {
            var info = CreateInfo();
            if (info is null)
                return null;

            if (infos is null && _cacheByPgTypeId.TryAdd(pgTypeId, new[] { info }))
                return info;

            infos ??= _cacheByPgTypeId[pgTypeId];
            while (true)
            {
                foreach (var cachedInfo in infos)
                    if (type is null && cachedInfo.IsDefault || cachedInfo.Type == type)
                        return cachedInfo;

                var oldLength = infos.Length;
                var oldInfos = infos;
                Array.Resize(ref infos, infos.Length + 1);
                infos[oldLength] = info;
                if (!_cacheByPgTypeId.TryUpdate(pgTypeId, infos, oldInfos))
                    infos = _cacheByPgTypeId[pgTypeId];
                else
                    return info;
            }
        }

        PgConverterInfo? CreateInfo()
        {
            var typeId = AsPgTypeId(pgTypeId);
            DataTypeName? dataTypeName = null;
            if (typeId is { } id)
                dataTypeName = id.IsDataTypeName ? id.DataTypeName : _options.TypeCatalog.GetDataTypeName(id);
            var info = _options.ConverterInfoResolver.GetConverterInfo(type, dataTypeName, _options);
            if (info is null)
                return null;

            if (type is null)
            {
                if (!info.IsDefault)
                    throw new InvalidOperationException("No CLR type was passed but the resolved PgConverterInfo does not have IsDefault set to true.");
            }
            else
            {
                if (pgTypeId is null && !info.IsDefault)
                    throw new InvalidOperationException("No DataTypeName was passed but the resolved PgConverterInfo does not have IsDefault set to true.");
                if (info.Type != type)
                    throw new InvalidOperationException("A CLR type was passed but the resolved PgConverterInfo is not for this type.");
            }

            // We can't do this for ValueDependent infos but it's a good sanity check.
            if (pgTypeId is not null && info.PgTypeId is { } infoTypeId && infoTypeId != typeId)
                throw new InvalidOperationException("No DataTypeName was passed but the resolved PgConverterInfo does not have IsDefault set to true.");

            return info;
        }
    }
}

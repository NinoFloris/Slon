using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Slon.Pg.Types;

namespace Slon.Pg;

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

        return AddEntryById(id, infos);

        PgConverterInfo? AddByType(Type type)
        {
            var info = CreateInfo(type, pgTypeId, _options);
            if (info is null)
                return null;

            // We never remove entries so either of these branches will always succeed.
            return _cacheByClrType.TryAdd(type, info) ? info : _cacheByClrType[type];
        }

        PgConverterInfo? AddEntryById(TPgTypeId pgTypeId, PgConverterInfo[]? infos)
        {
            var info = CreateInfo(type, pgTypeId, _options);
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

        static PgConverterInfo? CreateInfo(Type? type, TPgTypeId? pgtypeid, PgConverterOptions options)
        {
            var typeId = AsPgTypeId(pgtypeid);
            var info = options.ConverterInfoResolver.GetConverterInfo(type, typeId is { } id ? options.TypeCatalog.GetDataTypeName(id) : null, options);
            if (info is null)
                return null;

            if (typeId is not null)
            {
                if (info.PgTypeId != typeId)
                    throw new InvalidOperationException("A Postgres type was passed but the resolved PgConverterInfo does not have an equal PgTypeId.");

                if (type is null && !info.IsDefault)
                    throw new InvalidOperationException("No CLR type was passed but the resolved PgConverterInfo does not have IsDefault set to true.");
            }

            if (type is not null)
            {
                if (info.Type != type)
                    throw new InvalidOperationException("A CLR type was passed but the resolved PgConverterInfo does not have an equal Type.");

                if (typeId is null && !info.IsDefault)
                    throw new InvalidOperationException("No Postgres type was passed but the resolved PgConverterInfo does not have IsDefault set to true.");
            }

            return info;
        }

        static PgTypeId? AsPgTypeId(TPgTypeId? pgTypeId)
        {
            if (pgTypeId is { } id)
                return (typeof(TPgTypeId) == typeof(DataTypeName)) switch
                {
                    true => new PgTypeId(Unsafe.As<TPgTypeId, DataTypeName>(ref id)),
                    false => new PgTypeId(Unsafe.As<TPgTypeId, Oid>(ref id))
                };

            return null;
        }
    }
}

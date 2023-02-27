using System;
using System.Buffers;
using System.Collections.Generic;
using Slon.Data;
using Slon.Pg;
using Slon.Pg.Types;
using Slon.Protocol.Pg;

namespace Slon;

delegate ParameterContextBuilder ParameterContextBuilderFactory(int length, int bufferSize);

// TODO Can imagine threading through something like Variant from https://github.com/dotnet/runtime/blob/main/src/coreclr/System.Private.CoreLib/src/System/Variant.cs
// Would remove all boxing for primitives end-to-end (when supported in DbParameterCollection as well).
readonly struct ParameterContextFactory
{
    readonly IFrontendTypeCatalog _frontendTypeCatalog;
    readonly IFacetsTransformer _facetsTransformer;
    readonly ParameterContextBuilderFactory _builderFactory;
    readonly int _estimatedParameterBufferSize;

    public ParameterContextFactory(IFrontendTypeCatalog frontendTypeCatalog, IFacetsTransformer facetsTransformer, ParameterContextBuilderFactory builderFactory, int estimatedParameterBufferSize)
    {
        _frontendTypeCatalog = frontendTypeCatalog;
        _facetsTransformer = facetsTransformer;
        _builderFactory = builderFactory;
        _estimatedParameterBufferSize = estimatedParameterBufferSize;
    }

    ParameterCacheItem AddParameter(ref ParameterContextBuilder builder, KeyValuePair<string, object?> slonParameter, IParameterSession? session, PgConverterInfo? cachedConverterInfo = null)
    {
        if (slonParameter.Key is not "")
            throw new NotSupportedException("Named parameter sql rewriting is not implemented yet, use positional parameters instead.");

        if (slonParameter.Value is SlonDbParameter dbParameter)
        {
            if (session is null)
                throw new ArgumentNullException(nameof(session), "Need a non-null session for SlonDbParameter parameter values.");

            return AddParameter(ref builder, dbParameter, session, cachedConverterInfo);
        }

        return AddParameter(ref builder, slonParameter.Value, cachedConverterInfo);
    }

    DataRepresentation? GetRepresentation(bool? preferTextualRepresentation) =>
        preferTextualRepresentation switch
        {
            true => DataRepresentation.Text,
            false => DataRepresentation.Binary,
            null => null
        };

    ParameterCacheItem AddParameter(ref ParameterContextBuilder builder, SlonDbParameter dbParameter, IParameterSession session, PgConverterInfo? cachedConverterInfo = null)
    {
        var cacheItem = new ParameterCacheItem();
        cacheItem.IsSlonDbParameter = true;
        cacheItem.PreferTextualRepresentation = dbParameter.PreferTextualRepresentation;
        cacheItem.ValueRevision = dbParameter.ValueRevision;
        cacheItem.ValueType = dbParameter.ValueType;
        cacheItem.Value = dbParameter;
        PgTypeId? pgTypeId = null;
        if (dbParameter.GetExplicitDbType() is { } slonDbType)
        {
            if (!_frontendTypeCatalog.TryGetIdentifiers(slonDbType, out var id, out var dataTypeName))
                throw new InvalidOperationException("Could not resolve given DbType to a known data type.");

            pgTypeId = id;
            cacheItem.DbType = new SlonDbType(dataTypeName);
        }
        // We allow generic db parameters to have null values without specifying some identifier, as we know the CLR type.
        else if (cacheItem.ValueType is null)
            throw new InvalidOperationException($"A null value requires an {nameof(SlonDbType)} to be set.");
        else
            cacheItem.IsInferredDbType = true;

        Parameter parameter;
        if (cachedConverterInfo is not null)
            parameter = builder.AddParameter(session, cachedConverterInfo, GetRepresentation(cacheItem.PreferTextualRepresentation));
        else
            parameter = builder.AddParameter(session, pgTypeId, GetRepresentation(cacheItem.PreferTextualRepresentation));

        // No sense in caching (potentially) single use sessions.
        cacheItem.Parameter = parameter with { Value = null };

        if (cacheItem.IsInferredDbType)
        {
            // Inferred db types don't need a lookup for 'correctness' as they are already fully qualified,
            // they're only settable by internals, and not at all used for converter info resolving.
            // It only serves to communicate back to the user what final db type was chosen.
            // TODO we may want to add an api to/for SlonDbParameter to do inference without execution.
            if (dbParameter.HasInferredSlonDbType)
                cacheItem.DbType = dbParameter.SlonDbType;
            else
            {
                cacheItem.DbType = new SlonDbType(_frontendTypeCatalog.GetDataTypeName(parameter.PgTypeId));
                dbParameter.SetInferredDbType(cacheItem.DbType, parameter.ConverterInfo.IsValueDependent);
            }
        }

        return cacheItem;
    }

    ParameterCacheItem AddParameter(ref ParameterContextBuilder builder, object? value, PgConverterInfo? cachedConverterInfo = null)
        => new()
        {
            Value = value,
            ValueType = value?.GetType(),
            Parameter = builder.AddParameter(value, converterInfo: cachedConverterInfo)
        };

    (ParameterContext, ParameterCache?) CreateFromCache(ReadOnlySpan<ParameterCacheItem> cachedParameters, SlonParameterCollection parameters)
    {
        using var enumerator = parameters.GetValueEnumerator();
        ParameterCacheItem[]? updatedCacheArray = null;
        ParameterCache? updatedCache = null;
        IParameterSession? lastSession = null;
        var builder = _builderFactory(parameters.Count, _estimatedParameterBufferSize);
        var i = 0;
        try
        {
            for (; i < cachedParameters.Length; i++)
            {
                enumerator.MoveNext();
                ref readonly var cacheItem = ref cachedParameters[i];
                // Start a session right away to be sure we keep a consistent view.
                lastSession = enumerator.Current.Value is SlonDbParameter dbParameter ? dbParameter.StartSession(_facetsTransformer) : null;

                switch (cacheItem.TryGetParameter(enumerator.Current, out var cachedParameter, out var cachedConverterInfo))
                {
                    case ParameterEquality.Full:
                        // We sync the InferredDbType if the current instance does not have it yet, this saves a lookup.
                        if (cacheItem.IsSlonDbParameter)
                        {
                            dbParameter = (SlonDbParameter)enumerator.Current.Value!;
                            if (cacheItem.IsInferredDbType && dbParameter.HasInferredSlonDbType == false && dbParameter.SlonDbType == SlonDbType.Infer)
                                dbParameter.SetInferredDbType(cacheItem.DbType, cachedParameter.ConverterInfo.IsValueDependent);

                            // If our value is an SlonDbParameter we have to use our fresh session as the value.
                            cachedParameter = cachedParameter with { Value = lastSession };
                        }

                        builder.AddParameter(cachedParameter);
                        break;
                    case ParameterEquality.ConverterInfo:
                        // We sync the InferredDbType if the current instance does not have it yet, this saves a lookup.
                        if (cacheItem.IsSlonDbParameter)
                        {
                            dbParameter = (SlonDbParameter)enumerator.Current.Value!;
                            if (cacheItem.IsInferredDbType && dbParameter.HasInferredSlonDbType == false && dbParameter.SlonDbType == SlonDbType.Infer)
                                dbParameter.SetInferredDbType(cacheItem.DbType, cachedParameter.ConverterInfo.IsValueDependent);
                        }

                        // We don't update the cache for converter info matches as it does not seem worth the cost
                        // Having the converter info ready right away is the big win.
                        AddParameter(ref builder, enumerator.Current, lastSession, cachedConverterInfo);
                        break;
                    case ParameterEquality.None:
                        // We support incremental cache changes but we cannot change the original array, we may create torn reads otherwise.
                        if (updatedCacheArray is null)
                        {
                            updatedCacheArray ??= ArrayPool<ParameterCacheItem>.Shared.Rent(parameters.Count);
                            updatedCache ??= new ParameterCache(new PooledMemory<ParameterCacheItem>(updatedCacheArray, parameters.Count), builder.Revision);
                            cachedParameters.CopyTo(updatedCacheArray);
                        }

                        var updatedCacheItem = AddParameter(ref builder, enumerator.Current, lastSession);
                        updatedCacheArray[i] = updatedCacheItem;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }
        catch
        {
            Cleanup(builder, lastSession, i);
            throw;
        }

        return (builder.Build(), updatedCache);
    }

    public (ParameterContext Context, ParameterCache? Cache) Create(SlonParameterCollection parameters, ParameterCache cache = default, bool createCache = true)
    {
        var builder = _builderFactory(parameters.Count, _estimatedParameterBufferSize);
        if (!cache.IsDefault && cache.TryGetItems(builder, out var items))
            return CreateFromCache(items, parameters);

        var cacheArray = createCache ? ArrayPool<ParameterCacheItem>.Shared.Rent(parameters.Count) : default!;
        cache = createCache ? new ParameterCache(new PooledMemory<ParameterCacheItem>(cacheArray, parameters.Count), builder.Revision) : default;
        var i = 0;
        IParameterSession? lastSession = null;
        try
        {
            foreach (var kv in parameters.GetValueEnumerator())
            {
                lastSession = kv.Value is SlonDbParameter dbParameter ? dbParameter.StartSession(_facetsTransformer) : null;
                var cacheItem = AddParameter(ref builder, kv, lastSession);
                if (createCache)
                    cacheArray[i++] = cacheItem;
            }
        }
        catch
        {
            Cleanup(builder, lastSession, i);
            throw;
        }

        return (builder.Build(), cache);
    }

    void Cleanup(ParameterContextBuilder builder, IParameterSession? lastSession, int index)
    {
        // If builder length is equal it didn't get added before throwing, close it.
        // We do it this way to make sure we don't do any double frees.
        // ReferenceEquals would fail if the same parameter instance is used for multiple parameters.
        if (builder.Items.Length == index)
            lastSession?.Close();

        foreach (var p in builder.Items)
        {
            if (p.TryGetParameterSession(out var session))
                session.Close();
        }
    }
}

readonly struct ParameterCache : IDisposable
{
    readonly PooledMemory<ParameterCacheItem> _pooledMemory;

    public ParameterCache(PooledMemory<ParameterCacheItem> pooledMemory, int builderRevision)
    {
        _pooledMemory = pooledMemory;
        BuilderRevision = builderRevision;
    }

    int BuilderRevision { get; }
    ReadOnlySpan<ParameterCacheItem> Items => _pooledMemory.Span;

    static void ThrowDefaultValue() => throw new InvalidOperationException($"This operation cannot be performed on a default value of {nameof(ParameterCache)}.");

    public bool IsDefault => _pooledMemory.IsDefault;
    public bool TryGetItems(ParameterContextBuilder builder, out ReadOnlySpan<ParameterCacheItem> items)
    {
        if (IsDefault || BuilderRevision != builder.Revision || _pooledMemory.Length != builder.Length)
        {
            if (IsDefault)
                ThrowDefaultValue();

            items = default;
            return false;
        }

        items = Items;
        return true;
    }

    public void Dispose() => _pooledMemory.Dispose();
}

enum ParameterEquality
{
    None,
    ConverterInfo,
    Full,
}

struct ParameterCacheItem
{
    public Type? ValueType { get; set; }
    public object? Value { private get; set; }
    public Parameter Parameter { private get; set; }

    // Only used with SlonDbParameters
    public bool IsSlonDbParameter { get; set; }
    public short ValueRevision { get; set; }
    public bool? PreferTextualRepresentation { get; set; }
    public SlonDbType DbType { get; set; }
    public bool IsInferredDbType { get; set; }

    public readonly ParameterEquality TryGetParameter(object? value, out Parameter cachedParameter, out PgConverterInfo? cachedConverterInfo)
    {
        var parameterEquality = GetParameterEquality(value);
        if (parameterEquality is ParameterEquality.None)
        {
            cachedParameter = default;
            cachedConverterInfo = null;
            return ParameterEquality.None;
        }

        if (parameterEquality is ParameterEquality.ConverterInfo)
        {
            cachedParameter = default;
            cachedConverterInfo = Parameter.ConverterInfo;
            return ParameterEquality.ConverterInfo;
        }

        cachedParameter = Parameter;
        cachedConverterInfo = null;
        return ParameterEquality.Full;
    }

    readonly ParameterEquality GetParameterEquality(object? value)
    {
        var cachedValue = Value;
        // We support structural equality of ado.net parameters, doing so requires some careful equality handling though.
        if (IsSlonDbParameter && (SlonDbParameter)cachedValue! is var cachedDbParameter && value is SlonDbParameter dbParameter)
        {
            if (!ConverterInfoEquals(null, cachedDbParameter, dbParameter))
                return ParameterEquality.None;

            // From this point on we can recompute any missing pieces with the cached converter info.
            // If the value of cachedDbParameter was mutated over time we catch that by checking the current ValueRevision against the stored copy.
            if (dbParameter.PreferTextualRepresentation != PreferTextualRepresentation ||
                cachedDbParameter.ValueRevision != ValueRevision || (!ReferenceEquals(cachedDbParameter, dbParameter) && !cachedDbParameter.ValueEquals(dbParameter)))
                return ParameterEquality.ConverterInfo;

            // TODO if Revision wrapped around (so value changed 65536 times) and ends up at the same value as the value we're checking
            // TODO (but not the value we stored info on) we'll fail to detect it, probably such an edge-case it won't ever happen...

            // When values are fully equal we can reuse pgtypeid, precomputed sizes, and flags as well.
            return ParameterEquality.Full;
        }

        // Shortcircuit if it's the same instance.
        if (ReferenceEquals(cachedValue, value))
            return ParameterEquality.Full;

        if (!ConverterInfoEquals(value, null, null))
            return ParameterEquality.None;

        if (!Equals(cachedValue, value))
            return ParameterEquality.ConverterInfo;

        return ParameterEquality.Full;
    }

    readonly bool ConverterInfoEquals(object? value, SlonDbParameter? cachedDbParameter, SlonDbParameter? dbParameter)
    {
        // Converter info is resolved based on the Type of the value and an optional PgTypeId (which is resolved from a SlonDbType).
        // Note: we don't use the values stored on the cachedDbParameter as these may have been mutated, instead we refer to copies.
        if (cachedDbParameter is not null && dbParameter is not null)
        {
            if (!IsInferredDbType && (dbParameter.HasInferredSlonDbType || dbParameter.SlonDbType != DbType))
                return false;

            if (dbParameter.ValueType != ValueType)
                return false;

            return true;
        }

        // For directly boxed values we only have to check the clr type.
        return value?.GetType() == ValueType;
    }
}

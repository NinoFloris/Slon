using System;
using Npgsql.Pipelines.Data;
using Npgsql.Pipelines.Pg;
using Npgsql.Pipelines.Pg.Types;
using Npgsql.Pipelines.Protocol.Pg;
using Npgsql.Pipelines.Protocol.PgV3;
using NUnit.Framework;

namespace Npgsql.Pipelines.Tests;

public class NpgsqlParameterTests
{
    // Mock for the relevant pieces from NpgsqlDataSource
    class FrontendTypeCatalog: IFrontendTypeCatalog
    {
        public PgTypeCatalog TypeCatalog { get; }

        public FrontendTypeCatalog(PgTypeCatalog typeCatalog) => TypeCatalog = typeCatalog;
        public DataTypeName GetDataTypeName(PgTypeId pgTypeId) => TypeCatalog.GetDataTypeName(pgTypeId);

        public bool TryGetIdentifiers(NpgsqlDbType npgsqlDbType, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
            => NpgsqlDataSource.PgDbDependencies.TryGetIdentifiers(TypeCatalog, npgsqlDbType, out canonicalTypeId, out dataTypeName);
    }

    static readonly FrontendTypeCatalog FrontendCatalog = new(PgTypeCatalog.Default);
    static readonly FrontendTypeCatalog FrontendPortableCatalog = new(PgTypeCatalog.Default.ToPortableCatalog());

    static ParameterContextBuilderFactory CreateBuilderFactory(int revision, bool portableTypeCatalog)
        => (length, bufferSize) => new ParameterContextBuilder(length, bufferSize, revision,
            new PgConverterOptions
            {
                TypeCatalog = portableTypeCatalog ? FrontendPortableCatalog.TypeCatalog : FrontendCatalog.TypeCatalog,
                TextEncoding = PgOptions.DefaultEncoding,
                ConverterInfoResolver = new DefaultConverterInfoResolver()
            });
    static ParameterContextFactory CreateFactory(int revision, string? statementText = null, bool portableTypeCatalog = false) => new(
        FrontendCatalog,
        new IdentityFacetsTransformer(),
        CreateBuilderFactory(revision, portableTypeCatalog),
        PgV3CommandWriter.EstimateParameterBufferSize(PgStreamConnection.WriterSegmentSize, statementText)
    );

    static readonly string DefaultStatementText = "SELECT 1 FROM table;";
    static readonly int DefaultBufferSize = PgV3CommandWriter.EstimateParameterBufferSize(PgStreamConnection.WriterSegmentSize, DefaultStatementText);
    static readonly ParameterContextBuilderFactory DefaultBuilderFactory = CreateBuilderFactory(1, false);
    static readonly ParameterContextFactory DefaultFactory = CreateFactory(1, DefaultStatementText);

    // TODO test disposal/close semantics of parameters to make sure close always happens and never twice.

    [Test]
    public void ValueParameterDataCongruence()
    {
        var result = DefaultFactory.Create(new NpgsqlParameterCollection
        {
            1,
        }, createCache: true);

        var items = CheckParametersAndCache(result.Context, result.Cache, 1);
        var parameter = result.Context.Parameters.Span[0];
        var cacheItem = items[0];
        CheckCommonParameterDataCongruence<int>(parameter, cacheItem);

        // We don't do any lookup back to a datatypename for value parameters.
        Assert.False(cacheItem.IsNpgsqlDbParameter);
        Assert.False(cacheItem.IsInferredNpgsqlDbType);
        Assert.AreEqual(NpgsqlDbType.Infer, cacheItem.NpgsqlDbType);
    }

    [Test]
    public void DbParameterDataCongruence()
    {
        var dbParameter = new NpgsqlParameter<int>(1);
        var result = DefaultFactory.Create(new NpgsqlParameterCollection
        {
            dbParameter
        }, createCache: true);

        var items = CheckParametersAndCache(result.Context, result.Cache, 1);
        var parameter = result.Context.Parameters.Span[0];
        var cacheItem = items[0];
        CheckCommonParameterDataCongruence<int>(parameter, cacheItem);

        // Check all the inference lookup happened correctly.
        var dataTypeName = parameter.ConverterInfo.Options.TypeCatalog.GetDataTypeName(parameter.PgTypeId);
        Assert.True(cacheItem.IsInferredNpgsqlDbType);
        Assert.True(dbParameter.HasInferredNpgsqlDbType);
        Assert.AreEqual(new NpgsqlDbType(dataTypeName), cacheItem.NpgsqlDbType);
        Assert.AreEqual(dataTypeName, new DataTypeName(dbParameter.NpgsqlDbType.DataTypeName));
    }

    [Test]
    public void PgTypeDbParameterDataCongruence()
    {
        var dbParameter = new NpgsqlParameter<int>(1) { NpgsqlDbType = NpgsqlDbTypes.Int2 };
        var result = DefaultFactory.Create(new NpgsqlParameterCollection
        {
            dbParameter
        }, createCache: true);

        var items = CheckParametersAndCache(result.Context, result.Cache, 1);
        var parameter = result.Context.Parameters.Span[0];
        var cacheItem = items[0];
        CheckCommonParameterDataCongruence<int>(parameter, cacheItem);

        // Check all the typed lookup happened correctly
        var dataTypeName = new DataTypeName(NpgsqlDbTypes.Int2.DataTypeName);
        Assert.False(cacheItem.IsInferredNpgsqlDbType);
        Assert.False(dbParameter.HasInferredNpgsqlDbType);
        Assert.AreEqual(new NpgsqlDbType(dataTypeName), cacheItem.NpgsqlDbType);
        Assert.AreEqual(dataTypeName, new DataTypeName(dbParameter.NpgsqlDbType.DataTypeName));
    }

    static ReadOnlySpan<ParameterCacheItem> CheckParametersAndCache(ParameterContext context, ParameterCache? maybeCache, int expectedLength)
    {
        Assert.AreEqual(expectedLength, context.Parameters.Length);
        Assert.NotNull(maybeCache);
        var cache = maybeCache.GetValueOrDefault();
        Assert.True(cache.TryGetItems(DefaultBuilderFactory(expectedLength, 8000), out var items));
        Assert.AreEqual(expectedLength, items.Length);
        return items;
    }

    static void CheckCommonParameterDataCongruence<T>(Parameter parameter, ParameterCacheItem cacheItem)
    {
        Assert.AreEqual(typeof(T), cacheItem.ValueType);
        Assert.AreEqual(ParameterEquality.Full, cacheItem.TryGetParameter(parameter.Value, out var cachedParameter, out var cachedConverterInfo));
        // Check the cached values against the parameter we have.
        if (cacheItem.IsNpgsqlDbParameter)
            // Check that we don't cache parameter sessions.
            Assert.Null(cachedParameter.Value);
        else
            Assert.AreEqual(parameter.Value, cachedParameter.Value);
        Assert.AreEqual(parameter.ConverterInfo, cachedParameter.ConverterInfo);
        Assert.AreEqual(parameter.Size, cachedParameter.Size);
        Assert.AreEqual(parameter.PgTypeId, cachedParameter.PgTypeId);
        Assert.AreEqual(parameter.IsDbNull, cachedParameter.IsDbNull);
        Assert.AreEqual(parameter.DataRepresentation, cachedParameter.DataRepresentation);
        Assert.AreEqual(parameter.WriteState is null, cachedParameter.WriteState is null); // Instances will be unique, can't expect equality.

        // Check the original against a freshly created parameter.
        var expectedP = parameter.ConverterInfo.CreateParameter(parameter.Value, DefaultBufferSize, true, parameter.DataRepresentation);
        Assert.AreEqual(expectedP.ConverterInfo, parameter.ConverterInfo);
        Assert.AreEqual(expectedP.Value, parameter.Value);
        Assert.AreEqual(expectedP.Size, parameter.Size);
        Assert.AreEqual(expectedP.PgTypeId, parameter.PgTypeId);
        Assert.AreEqual(expectedP.IsDbNull, parameter.IsDbNull);
        Assert.AreEqual(expectedP.DataRepresentation, parameter.DataRepresentation);
        Assert.AreEqual(expectedP.WriteState is null, parameter.WriteState is null); // Instances will be unique, can't expect equality.
    }
}

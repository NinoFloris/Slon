using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using NUnit.Framework;

namespace Npgsql.Pipelines.Data.Tests;

public class DbDataParameterCollectionTests
{
    const int LookupThreshold = PositionalTestCollection.LookupThreshold;
    const string PositionalName = PositionalTestCollection.PositionalName;

    class PositionalTestCollection : DbDataParameterCollection<TestParameter>
    {
        public PositionalTestCollection() {}

        public void Add<T>(T? value) => AddCore(PositionalName, value);

        protected override bool CanParameterBePositional => true;
        protected override TestParameter CreateParameter(string parameterName, object? value)
            => new() { ParameterName = parameterName, Value = value };

        protected override TestParameter CreateParameter<T>(string parameterName, T? value) where T : default
            => new() { ParameterName = parameterName, Value = value };
    }

    class UnknownDbParameter : DbParameter
    {
        public override void ResetDbType() => throw new NotImplementedException();
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; }
        public override bool IsNullable { get; set; }
        [AllowNull] public override string ParameterName { get; set; } = "";
        [AllowNull] public override string SourceColumn { get; set; } = "";
        public override object? Value { get; set; }
        public override bool SourceColumnNullMapping { get; set; }
        public override int Size { get; set; }
    }

    [Test]
    public void UnknownDbParameterThrows()
    {
        var collection = new PositionalTestCollection();
        Assert.Throws<InvalidCastException>(() => collection.Add(new UnknownDbParameter()));
    }

    [Test]
    public void ClearSucceeds()
    {
        var collection = new PositionalTestCollection
        {
            { "test1", 1 },
            { "test2", "value" }
        };
        collection.Clear();
        Assert.False(collection.Contains("test1"));
        Assert.False(collection.Contains("test2"));
    }

    [Test]
    public void IndexOfFindsPrefixedNames()
    {
        var collection = new PositionalTestCollection
        {
            { "@p0", 1 },
            { ":p1", 1 },
            { "p2", 1 }
        };

        for (var i = 0; i < collection.Count; i++)
        {
            Assert.AreEqual(i, collection.IndexOf("@p" + i));
            Assert.AreEqual(i, collection.IndexOf(":p" + i));
            Assert.AreEqual(i, collection.IndexOf("p" + i));
        }
    }

    [Test]
    public void CaseInsensitiveLookups([Values(LookupThreshold, LookupThreshold - 2)] int count)
    {
        var collection = new PositionalTestCollection();
        for (var i = 0; i < count; i++)
            collection.Add($"p{i}", i);

        Assert.AreEqual(1, collection.IndexOf("P1"));
    }

    [Test]
    public void CaseSensitiveLookups([Values(LookupThreshold, LookupThreshold - 2)] int count)
    {
        var collection = new PositionalTestCollection();
        for (var i = 0; i < count; i++)
            collection.Add($"p{i}", i);

        Assert.AreEqual(1, collection.IndexOf("p1"));
    }

    [Test]
    public void PositionalLookup([Values(LookupThreshold, LookupThreshold - 2)] int count)
    {
        var collection = new PositionalTestCollection();
        for (var i = 0; i < count; i++)
            collection.Add(PositionalName, i);

        Assert.AreEqual(0, collection.IndexOf(""));
    }

    [Test]
    public void IndexerNameParameterNameMismatchThrows([Values(LookupThreshold, LookupThreshold - 2)] int count)
    {
        var collection = new PositionalTestCollection();
        for (var i = 0; i < count; i++)
            collection.Add($"p{i}", i);

        Assert.DoesNotThrow(() =>
        {
            collection["p1"] = new TestParameter { ParameterName = "p1", Value = 1};
            collection["p1"] = new TestParameter { ParameterName = "P1", Value = 1};
        });

        Assert.Throws<ArgumentException>(() =>
        {
            collection["p1"] = new TestParameter { ParameterName = "p2", Value = 1};
        });
    }
}

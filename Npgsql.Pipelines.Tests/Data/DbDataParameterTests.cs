using System;
using System.Data;
using NUnit.Framework;

namespace Npgsql.Pipelines.Data.Tests;

public class DbDataParameterTests
{
    [Test]
    public void ParameterNameDefaultEmptyString()
    {
        var p = new TestParameter();
        Assert.AreEqual(p.ParameterName, "");
    }

    [Test]
    public void DirectionDefaultInput()
    {
        var p = new TestParameter();
        Assert.AreEqual(p.Direction, ParameterDirection.Input);
    }

    [Test]
    public void FrozenNameThrows()
    {
        var p = new TestParameter() { ParameterName = "Name" };
        p.NotifyCollectionAdd();
        Assert.Throws<InvalidOperationException>(() => p.ParameterName = "NewName");
    }

    [Test]
    public void IncrementInUse()
    {
        var p = new TestParameter();
        p.Increment();
        Assert.IsTrue(p.InUse);
    }

    [Test]
    public void IncrementTo24bits()
    {
        var p = new TestParameter();
        Assert.DoesNotThrow(() => p.Increment((int)Math.Pow(2, 24) - 1));
    }

    [Test]
    public void DecrementInUse()
    {
        var p = new TestParameter();
        p.Increment();
        p.Decrement();
        Assert.IsTrue(!p.InUse);
    }

    [Test]
    public void IncrementPast24bitsThrows()
    {
        var p = new TestParameter();
        Assert.Throws<InvalidOperationException>(() => p.Increment(int.MaxValue));
    }

    [Test]
    public void DecrementPastZeroThrows()
    {
        var p = new TestParameter();
        Assert.Throws<InvalidOperationException>(() => p.Decrement());
    }

    [Test]
    public void DecrementFrom24bitsPastZeroThrows()
    {
        var p = new TestParameter();
        p.Increment((int)Math.Pow(2, 24) - 1);
        Assert.Throws<InvalidOperationException>(() => p.Decrement(int.MaxValue));
    }

    [Test]
    public void MixedUseOfCombinedFieldWorks()
    {
        var p = new TestParameter();
        p.Direction = ParameterDirection.InputOutput;
        p.IsNullable = true;
        p.Increment((int)Math.Pow(2, 24) - 1);
        p.NotifyCollectionAdd();
        Assert.IsTrue(p.InUse);
        Assert.AreEqual(p.Direction, ParameterDirection.InputOutput);
        Assert.IsTrue(p.IsNullable);

        p.Decrement((int)Math.Pow(2, 24) - 1);
        p.Direction = ParameterDirection.Output;
        p.IsNullable = false;
        Assert.AreEqual(p.Direction, ParameterDirection.Output);
        Assert.IsFalse(p.IsNullable);
        Assert.IsFalse(p.InUse);
        Assert.Throws<InvalidOperationException>(() => p.ParameterName = "NewName");
    }

    [Test]
    public void MutationWhileInUseThrows()
    {
        var p = new TestParameter();
        p.Direction = ParameterDirection.InputOutput;
        p.Increment();
        Assert.Throws<InvalidOperationException>(() => p.Value = "");
        Assert.Throws<InvalidOperationException>(() => p.Direction = ParameterDirection.Input);
        Assert.Throws<InvalidOperationException>(() => p.ParameterName = "");
        Assert.Throws<InvalidOperationException>(() => p.DbType = DbType.Binary);
        Assert.Throws<InvalidOperationException>(() => p.Size = 1);
        Assert.Throws<InvalidOperationException>(() => p.Scale = 1);
        Assert.Throws<InvalidOperationException>(() => p.Precision = 1);
        Assert.Throws<InvalidOperationException>(() => p.IsNullable = true);
        Assert.Throws<InvalidOperationException>(() => p.SourceColumn = "");
        Assert.Throws<InvalidOperationException>(() => p.SourceColumnNullMapping = true);
        Assert.Throws<InvalidOperationException>(() => p.SourceVersion = DataRowVersion.Current);
    }

    [Test]
    public void MutationAfterInUseWorks()
    {
        var p = new TestParameter();
        p.Direction = ParameterDirection.InputOutput;
        p.Increment();
        p.Decrement();
        p.Value = "";
        p.Direction = ParameterDirection.Input;
        p.ParameterName = "";
        p.DbType = DbType.Binary;
        p.Size = 1;
        p.Scale = 1;
        p.Precision = 1;
        p.IsNullable = true;
        p.SourceColumn = "";
        p.SourceColumnNullMapping = true;
        p.SourceVersion = DataRowVersion.Current;
    }

    [Test]
    public void CloneAllowsNameChange()
    {
        var p = new TestParameter();
        p.NotifyCollectionAdd();
        p = p.Clone();
        Assert.DoesNotThrow(() => p.ParameterName = "NewName");
    }

    [Test]
    public void CloneHasFacets()
    {
        var p = new TestParameter();
        p.Precision = 10;
        p.NotifyCollectionAdd();
        var clone = p.Clone();
        Assert.AreEqual(p.Precision, clone.Precision);
    }

    [Test]
    public void ValueTypeChangeResetsFacets()
    {
        var p = new TestParameter();
        p.Precision = 10;
        p.Value = 100;
        p.Value = "";
        Assert.AreEqual(p.Precision, 0);
    }

    [Test]
    public void InvalidDirectionThrows()
    {
        var p = new TestParameter();
        Assert.Throws<ArgumentOutOfRangeException>(() => p.Direction = (ParameterDirection)int.MaxValue);
    }

    [Test]
    public void InvalidDbTypeThrows()
    {
        var p = new TestParameter();
        Assert.Throws<ArgumentOutOfRangeException>(() => p.DbType = (DbType)int.MaxValue);
    }
}

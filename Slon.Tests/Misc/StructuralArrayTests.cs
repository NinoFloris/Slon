using System.Collections.Immutable;
using NUnit.Framework;

namespace Slon.Tests;

public class StructuralArrayTests
{
    [Test]
    public void StructuralEqualitySucceeds()
    {
        StructuralArray<string> left = ImmutableArray.Create<string>("a", "b");
        StructuralArray<string> right = ImmutableArray.Create<string>("a", "b");
        Assert.True(left.Equals(right));
    }

    [Test]
    public void StructuralEqualityFails()
    {
        StructuralArray<string> left = ImmutableArray.Create<string>("a", "b");
        StructuralArray<string> right = ImmutableArray.Create<string>("a", "c");
        Assert.False(left.Equals(right));
    }

    [Test]
    public void StructuralArrayReturnsOriginalImmutableArray()
    {
        var array = ImmutableArray.Create<string>("a", "b");
        StructuralArray<string> structural = array;

        Assert.True(array.Equals(structural.AsImmutableArray()));
    }
}

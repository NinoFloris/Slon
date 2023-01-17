namespace Npgsql.Pipelines.Benchmark;

using BenchmarkDotNet.Attributes;

public class Base
{
    public int Length { get; }
}

public sealed class Derived : Base
{

}

public class Casting
{
    private readonly object input = new Derived();
    private readonly string input2 = "woof";

    [Benchmark]
    public int BaseCast()
    {
        var text = (Base)input;
        return text.Length;
    }

    [Benchmark]
    public int IsBaseCast()
    {
        if(input is Base text)
        {
            return text.Length;
        }

        return 0;
    }

    [Benchmark]
    public int DerivedCast()
    {
        var text = (Derived)input;
        return text.Length;
    }

    [Benchmark]
    public int IsDerivedCast()
    {
        if(input is Derived text)
        {
            return text.Length;
        }

        return 0;
    }

    [Benchmark]
    public int Baseline()
    {
        if(input2 != null)
            return input2.Length;

        return 0;
    }
}

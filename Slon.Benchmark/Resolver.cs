using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using Slon.Pg;
using Slon.Pg.Converters;
using Slon.Pg.Types;

namespace Slon.Benchmark;

[Config(typeof(Config))]
public class Resolver
{
    class Config : ManualConfig
    {
        public Config()
        {
            // Add(new SimpleJobAttribute(targetCount: 20).Config);
            AddJob(new DebugInProcessConfig().GetJobs().First().UnfreezeCopy()
                .WithEnvironmentVariable("DOTNET_TieredPGO", "0")
                // .WithEnvironmentVariable("DOTNET_ThreadPool_UnfairSemaphoreSpinLimit", "0"));
                // .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_JitNoInline", "1"), new EnvironmentVariable("DOTNET_TieredCompilation", "0"))
            );
            // AddDiagnoser(new DisassemblyDiagnoser(new DisassemblyDiagnoserConfig()));
            AddColumn();
        }
    }

//     static readonly ConcurrentDictionary<Oid, PgConverterInfo[]> TypeLookup = new(concurrencyLevel: 1, 0, null);
//
//     class ThreadStaticCache<T>
//     {
//         [field: ThreadStatic]
//         public static PgConverterInfo<T>? Info { get; set; }
//     }
//
//     static readonly Type[] _types = { typeof(int), typeof(string), typeof(char), typeof(short), typeof(long), typeof(DateTime), typeof(decimal), typeof(JsonDocument), typeof(DateTimeOffset), typeof(float) };
//
//     [GlobalSetup(Targets = new []{ nameof(RandoConcDictLookup), nameof(ConcDictLookup)})]
//     public void Setup()
//     {
//         PgConverterInfo CreateInfo() => new PgConverterInfo<string>(null!, new StringTextConverter(new ReadOnlyMemoryTextConverter()));
//         PgConverterInfo CreateInfoArray() => new PgConverterInfo<string>(null!, new StringTextConverter(new ReadOnlyMemoryTextConverter()));
//         PgConverterInfo CreateInfoList() => new PgConverterInfo<string>(null!, new StringTextConverter(new ReadOnlyMemoryTextConverter()));
//
//         while (TypeLookup.Count < _types.Length * 3)
//         {
//             var tI = (uint)(Random.Shared.Next() % (_types.Length * 3));
//             TypeLookup[(Oid)(tI % _types.Length)] = new[] { CreateInfo() };
//             TypeLookup[(Oid)(tI % _types.Length + _types.Length * 1)] = new[] { CreateInfoArray() };
//             TypeLookup[(Oid)(tI % _types.Length + _types.Length * 2)] = new[] { CreateInfoList() };
//         }
//     }
//
//     // new TestPgConverterInfo<string>(_types[(tI + 1) % _types.Length]),
//     // new TestPgConverterInfo<string>(_types[(tI + 2) % _types.Length]),
//     // new TestPgConverterInfo<string>(_types[(tI + 3) % _types.Length]),
//     // new TestPgConverterInfo<string>(_types[(tI + 4) % _types.Length]),
//     // new TestPgConverterInfo<string>(_types[(tI + 5) % _types.Length]), new TestPgConverterInfo<string>(_types[(tI + 6) % _types.Length]), new TestPgConverterInfo<string>(_types[tI % _types.Length])
//
//     const int innerLoop = 32000000;
//
//     // [Benchmark(OperationsPerInvoke = innerLoop)]
//     public void RandoConcDictLookup()
//     {
//     //     var typesL = TypeLookup.Count;
//     //     var typesLen = _types.Length;
//     //     var types = _types;
//     //     for (int i = 0; i < innerLoop; i++)
//     //     {
//     //         var tI = (uint)(i % typesL);
//     //         var _ = TypeLookup[(Oid)tI][_types[tI % typesLen]];
//     //     }
//     }
//
//     [Benchmark(OperationsPerInvoke = innerLoop)]
//     public void ConcDictLookup()
//     {
//         PgConverterOptions options = null!;
//         for (int i = 0; i < innerLoop; i++)
//         {
//             var _ = LookupForUse(options);
//             // if (i % 10 == 0)
//             //     ThreadStaticCache<string>.Info = null;
//         }
//
//         [MethodImpl(MethodImplOptions.NoInlining)]
//         static PgConverter<string>? LookupForUse(PgConverterOptions converterOptions)
//         {
//             // Converter is PgConverter<string> converter
//
//             // // var tsInfo = ThreadStaticCache<string>.Info;
//             // if (tsInfo is not null && tsInfo.Options == converterOptions && tsInfo.Converter is PgConverter<string> converter)
//             // {
//             //     return converter;
//             // }
//
//             var globalInfo = TypeLookup[(Oid)1];
//             foreach (var typeInfo in globalInfo)
//             {
//                 if (typeInfo.Type == typeof(string))
//                     return (PgConverter<string>)typeInfo.Converter!;
//             }
//
//             return null;
//         }
//     }

    private static readonly (int, int)[] HeadByteDefinitions =
    {
        (1 << 7, 0b0000_0000),
        (1 << 5, 0b1100_0000),
        (1 << 4, 0b1110_0000),
        (1 << 3, 0b1111_0000)
    };

    static byte[] RandomUtf8Char(Random gen)
    {
        const int totalNumberOfUtf8Chars = (1 << 7) + (1 << 11) + (1 << 16) + (1 << 21);
        int tailByteCnt;
        var rnd = gen.Next(totalNumberOfUtf8Chars);
        if (rnd < (1 << 7))
            tailByteCnt = 0;
        else if (rnd < (1 << 7) + (1 << 11))
            tailByteCnt = 1;
        else if (rnd < (1 << 7) + (1 << 11) + (1 << 16))
            tailByteCnt = 2;
        else
            tailByteCnt = 3;

        var (range, offset) = HeadByteDefinitions[tailByteCnt];
        var headByte = Convert.ToByte(gen.Next(range) + offset);
        var tailBytes = Enumerable.Range(0, tailByteCnt)
            .Select(_ => Convert.ToByte(gen.Next(1 << 6) + 0b1000_0000));

        return new[] {headByte}.Concat(tailBytes).ToArray();
    }

    static string RandomString(Random gen, int length)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return new string(Enumerable.Repeat(chars, length)
            .Select(s => s[gen.Next(s.Length)]).ToArray());
    }

    // [Params(1,10,100,1000,10000)]
    // public int Length { get; set; }

    public string TestStr { get; set; }

    [GlobalSetup]
    public void CreateString()
    {
        // TestStr = string.Create(Length, (object?)null, (span, _) =>
        // {
        //     for (var i = 0; i < span.Length; i++)
        //     {
        //         var str = Encoding.UTF8.GetString(RandomUtf8Char(Random.Shared));
        //         for (var j = 0; j < str.Length; j++)
        //         {
        //             if (i >= span.Length)
        //                 break;
        //             span[i] = str[j];
        //             i++;
        //         }
        //     }
        // });

        // TestStr = RandomString(Random.Shared, Length);
;    }
    //
    // [Benchmark(OperationsPerInvoke = 1000)]
    // public void UpperBound()
    // {
    //     for (int i = 0; i < 1000; i++)
    //         Encoding.UTF8.GetMaxByteCount(TestStr.Length);
    // }
    //
    // [Benchmark(OperationsPerInvoke = 1000)]
    // public void Exact()
    // {
    //     for (int i = 0; i < 1000; i++)
    //         Encoding.UTF8.GetByteCount(TestStr);
    // }
    //
    //
    //
    abstract class Converter<T>
    {
        public abstract void Write(T value);
    }

    sealed class IntConverter: Converter<int>
    {
        int _value;

        public override void Write(int value) 
        { 
            // Store it so the JIT doesn't DCE it.
            _value = value * 3;
        }
    }

    abstract class ValueConverter<TIn, TOut, TConverter>: Converter<TIn>
        where TConverter: Converter<TOut>
    {
        protected readonly TConverter _converter;
        public ValueConverter(TConverter converter) => _converter = converter;

        protected abstract TOut ConvertTo(TIn value);
        public override void Write(TIn value) => _converter.Write(ConvertTo(value));
    }

    sealed class ShortConverter: ValueConverter<short, int, IntConverter>
    {
        readonly IntConverter _directConverter;

        public ShortConverter(IntConverter effectiveConverter) : base(effectiveConverter)
            => _directConverter = effectiveConverter;

        protected override int ConvertTo(short value)
        {
            return value * 100;
        }

        public void WriteDirect(short value) => _directConverter.Write(ConvertTo(value));

        [MethodImpl(MethodImplOptions.NoInlining)]
        public override void Write(short value) => base.Write(value);
    }

    static readonly ShortConverter Instance = new(new IntConverter());

    [Benchmark(OperationsPerInvoke = 1000)]
    public void UpperBound()
    {
        for (short i = 0; i < 1000; i++)
            Instance.Write(i);
    }

}

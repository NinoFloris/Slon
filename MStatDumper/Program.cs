using Mono.Cecil;
using Mono.Cecil.Rocks;

namespace MStatDumper
{
    internal class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                throw new Exception("Must provide the path to mstat file. It's in {project}/obj/Release/{TFM}/{os}/native/{project}.mstat");
            }

            var markDownStyleOutput = args.Length > 1 && args[1] == "md";

            var asm = AssemblyDefinition.ReadAssembly(args[0]);
            var globalType = (TypeDefinition)asm.MainModule.LookupToken(0x02000001);

            var types = globalType.Methods.First(x => x.Name == "Types");
            var typeStats = GetTypes(types).ToList();
            var typeSize = typeStats.Sum(x => x.Size);
            var typesByModules = typeStats.GroupBy(x => x.Type.Scope).Select(x => new { x.Key.Name, Sum = x.Sum(x => x.Size) }).ToList();
            if (markDownStyleOutput)
            {
                Console.WriteLine("<details>");
                Console.WriteLine($"<summary>Types Total Size {typeSize:n0}</summary>");
                Console.WriteLine();
                Console.WriteLine("<br>");
                Console.WriteLine();
                Console.WriteLine("| Name | Size |");
                Console.WriteLine("| --- | --- |");
                foreach (var m in typesByModules.OrderByDescending(x => x.Sum))
                {
                    var name = m.Name
                        .Replace("`", "\\`")
                        .Replace("<", "&#60;")
                        .Replace(">", "&#62;")
                        .Replace("|", "\\|");
                    Console.WriteLine($"| {name} | {m.Sum:n0} |");
                }
                Console.WriteLine();
                Console.WriteLine("</details>");
            }
            else
            {
                Console.WriteLine($"// ********** Types Total Size {typeSize:n0}");
                foreach (var m in typesByModules.OrderByDescending(x => x.Sum))
                {
                    Console.WriteLine($"{m.Name,-70} {m.Sum,9:n0}");
                }
                Console.WriteLine("// **********");
            }

            Console.WriteLine();

            var methods = globalType.Methods.First(x => x.Name == "Methods");
            var methodStats = GetMethods(methods).ToList();
            var methodSize = methodStats.Sum(x => x.Size + x.GcInfoSize + x.EhInfoSize);
            var methodsByModules = methodStats.GroupBy(x => x.Method.DeclaringType.Scope).Select(x => new { x.Key.Name, Sum = x.Sum(x => x.Size + x.GcInfoSize + x.EhInfoSize) }).ToList();
            if (markDownStyleOutput)
            {
                Console.WriteLine("<details>");
                Console.WriteLine($"<summary>Methods Total Size {methodSize:n0}</summary>");
                Console.WriteLine();
                Console.WriteLine("<br>");
                Console.WriteLine();
                Console.WriteLine("| Name | Size |");
                Console.WriteLine("| --- | --- |");
                foreach (var m in methodsByModules.OrderByDescending(x => x.Sum))
                {
                    var name = m.Name
                        .Replace("`", "\\`")
                        .Replace("<", "&#60;")
                        .Replace(">", "&#62;")
                        .Replace("|", "\\|");
                    Console.WriteLine($"| {name} | {m.Sum:n0} |");
                }
                Console.WriteLine();
                Console.WriteLine("</details>");
            }
            else
            {
                Console.WriteLine($"// ********** Methods Total Size {methodSize:n0}");
                foreach (var m in methodsByModules.OrderByDescending(x => x.Sum))
                {
                    Console.WriteLine($"{m.Name,-70} {m.Sum,9:n0}");
                }
                Console.WriteLine("// **********");
            }

            Console.WriteLine();

            string FindNamespace(TypeReference type)
            {
                var current = type;
                while (true)
                {
                    if (!string.IsNullOrEmpty(current.Namespace))
                    {
                        return current.Namespace;
                    }

                    if (current.DeclaringType == null)
                    {
                        return current.Name;
                    }

                    current = current.DeclaringType;
                }
            }

            var methodsByNamespace = methodStats.Select(x => new TypeStats { Type = x.Method.DeclaringType, Size = x.Size + x.GcInfoSize + x.EhInfoSize }).Concat(typeStats).GroupBy(x => FindNamespace(x.Type)).Select(x => new { x.Key, Sum = x.Sum(x => x.Size) }).ToList();
            if (markDownStyleOutput)
            {
                Console.WriteLine("<details>");
                Console.WriteLine("<summary>Size By Namespace</summary>");
                Console.WriteLine();
                Console.WriteLine("<br>");
                Console.WriteLine();
                Console.WriteLine("| Name | Size |");
                Console.WriteLine("| --- | --- |");
                foreach (var m in methodsByNamespace.OrderByDescending(x => x.Sum))
                {
                    var name = m.Key
                        .Replace("`", "\\`")
                        .Replace("<", "&#60;")
                        .Replace(">", "&#62;")
                        .Replace("|", "\\|");
                    Console.WriteLine($"| {name} | {m.Sum:n0} |");
                }
                Console.WriteLine();
                Console.WriteLine("</details>");
            }
            else
            {
                Console.WriteLine("// ********** Size By Namespace");
                foreach (var m in methodsByNamespace.OrderByDescending(x => x.Sum))
                {
                    Console.WriteLine($"{m.Key,-70} {m.Sum,9:n0}");
                }
                Console.WriteLine("// **********");
            }

            Console.WriteLine();

            var blobs = globalType.Methods.First(x => x.Name == "Blobs");
            var blobStats = GetBlobs(blobs).ToList();
            var blobSize = blobStats.Sum(x => x.Size);
            if (markDownStyleOutput)
            {
                Console.WriteLine("<details>");
                Console.WriteLine($"<summary>Blobs Total Size {blobSize:n0}</summary>");
                Console.WriteLine();
                Console.WriteLine("<br>");
                Console.WriteLine();
                Console.WriteLine("| Name | Size |");
                Console.WriteLine("| --- | --- |");
                foreach (var m in blobStats.OrderByDescending(x => x.Size))
                {
                    var name = m.Name
                        .Replace("`", "\\`")
                        .Replace("<", "&#60;")
                        .Replace(">", "&#62;")
                        .Replace("|", "\\|");
                    Console.WriteLine($"| {name} | {m.Size:n0} |");
                }
                Console.WriteLine();
                Console.WriteLine("</details>");
            }
            else
            {
                Console.WriteLine($"// ********** Blobs Total Size {blobSize:n0}");
                foreach (var m in blobStats.OrderByDescending(x => x.Size))
                {
                    Console.WriteLine($"{m.Name,-70} {m.Size,9:n0}");
                }
                Console.WriteLine("// **********");
            }

            if (markDownStyleOutput)
            {
                var methodsByScope = methodStats
                    .Where(x => x.Method.DeclaringType.Scope.Name == "Slon")
                    .ToArray();

                var methodsByClass = methodsByScope
                    .GroupBy(x => GetClassName(x.Method))
                    .OrderByDescending(x => x.Sum(x => x.Size + x.GcInfoSize + x.EhInfoSize))
                    .Take(200)
                    .ToArray();

                static string GetClassName(MethodReference methodReference)
                {
                    var type = methodReference.DeclaringType.DeclaringType ?? methodReference.DeclaringType;
                    return type.Namespace + "." + type.Name;
                }

                Console.WriteLine("<details>");
                Console.WriteLine("<summary>Top 200 Slon Classes By Methods Size</summary>");
                Console.WriteLine();
                Console.WriteLine("<br>");
                Console.WriteLine();
                Console.WriteLine("| Name | Size | Total Instantiations |");
                Console.WriteLine("| --- | --- | --- |");
                foreach (var m in methodsByClass
                             .Select(x => new { Name = x.Key, Sum = x.Sum(x => x.Size + x.GcInfoSize + x.EhInfoSize), Count = x.Count() })
                             .OrderByDescending(x => x.Sum))
                {
                    var name = m.Name
                        .Replace("`", "\\`")
                        .Replace("<", "&#60;")
                        .Replace(">", "&#62;")
                        .Replace("|", "\\|");
                    Console.WriteLine($"| {name} | {m.Sum:n0} | {m.Count} |");
                }

                Console.WriteLine();
                Console.WriteLine("<br>");

                foreach (var g in methodsByClass
                             .OrderByDescending(x => x.Sum(x => x.Size + x.GcInfoSize + x.EhInfoSize)))
                {
                    Console.WriteLine();
                    Console.WriteLine("<details>");
                    Console.WriteLine($"<summary>\"{g.Key}\" Methods ({g.Sum(x => x.Size + x.GcInfoSize + x.EhInfoSize):n0} bytes)</summary>");
                    Console.WriteLine();
                    Console.WriteLine("<br>");
                    Console.WriteLine();
                    Console.WriteLine("| Name | Size | Instantiations |");
                    Console.WriteLine("| --- | --- | --- |");
                    foreach (var m in g
                                 .GroupBy(x => GetMethodName(x.Method))
                                 .Select(x => new { Name = x.Key, Size = x.Sum(x => x.Size + x.GcInfoSize + x.EhInfoSize), Count = x.Count()})
                                 .OrderByDescending(x => x.Size))
                    {
                        var methodName = m.Name
                            .Replace("`", "\\`")
                            .Replace("<", "&#60;")
                            .Replace(">", "&#62;")
                            .Replace("|", "\\|");
                        Console.WriteLine($"| {methodName} | {m.Size:n0} | {m.Count} |");
                    }
                    Console.WriteLine();
                    Console.WriteLine("</details>");
                    Console.WriteLine();
                    Console.WriteLine("<br>");

                    static string GetMethodName(MethodReference methodReference)
                    {
                        if (methodReference.DeclaringType.DeclaringType is null)
                        {
                            return methodReference.Name;
                        }

                        return methodReference.DeclaringType.Name;
                    }
                }

                Console.WriteLine();
                Console.WriteLine("</details>");

                var filteredTypeStats = GetTypes(types)
                    .Where(x => x.Type.Scope.Name == "Slon")
                    // .GroupBy(x => x.Type.Name)
                    .OrderByDescending(x => x.Size)
                    .Take(1000)
                    .ToList();
                Console.WriteLine("<details>");
                Console.WriteLine("<summary>Top 200 Slon Types By Size</summary>");
                Console.WriteLine();
                Console.WriteLine("<br>");
                Console.WriteLine();
                Console.WriteLine("| Name | Size |");
                Console.WriteLine("| --- | --- |");
                foreach (var m in filteredTypeStats)
                {
                    var name = GetConcreteTypeName(m.Type, out _)
                        .Replace("`", "\\`")
                        .Replace("<", "&#60;")
                        .Replace(">", "&#62;")
                        .Replace("|", "\\|");
                    ;
                    Console.WriteLine($"| {name} | {m.Size:n0} |");
                }
                Console.WriteLine();
                Console.WriteLine("</details>");

                var filteredConverterTypeStats = GetTypes(types)
                    .Where(x => x.Type.Namespace.StartsWith("Slon.Pg"))
                    .ToArray();

                var filteredConverterMethods = methodsByScope
                    .Where(x => x.Method.DeclaringType.FullName.StartsWith("Slon.Pg"));

                var filteredConverterMethodStats =
                    filteredConverterMethods
                    .GroupBy(x => GetCanonTypeName(x.Method.DeclaringType, out _))
                    .Select(x => KeyValuePair.Create(x.Key, new { Size = x.Sum(x => x.Size + x.GcInfoSize + x.EhInfoSize), DeclaringType = x.First().Method.DeclaringType }))
                    .ToDictionary(x => x.Key, x => x.Value);

                var intermediateStats =
                    filteredConverterMethodStats.Join(filteredConverterTypeStats, x => x.Key, stats => GetCanonTypeName(stats.Type, out _), (methodStats, typeStats) =>
                        {
                            var type = typeStats.Type;
                            var canonName = GetCanonTypeName(type, out var hasCanonArg);
                            var concreteName = GetConcreteTypeName(type, out var concreteIsCanonOrDef);

                            // Whether this type is using shared methods.
                            var areCanonMethods = !concreteIsCanonOrDef && hasCanonArg;
                            return new
                            {
                                Type = type,
                                Name = concreteName,
                                // Whether this type is entirely instantiated over __Canon.
                                IsCanonTypeInstance = hasCanonArg && concreteIsCanonOrDef,
                                // Whether this type is actually just a type def.
                                // we consider these canon for purpose of method size attribution unless both canon and def exists which is handled later on.
                                IsTypeDef = !hasCanonArg && canonName.Contains("__Canon"),
                                IsMethodsViaCanon = areCanonMethods,
                                IsTypeViaCanon = false,
                                MethodSize = methodStats.Value.Size,
                                TypeSize = typeStats.Size,
                            };
                        })
                        .UnionBy(filteredConverterTypeStats.Select(x =>
                        {
                            var canonName = GetCanonTypeName(x.Type, out var hasCanonArg);
                            var concreteName = GetConcreteTypeName(x.Type, out var concreteIsCanonOrDef);
                            return new
                            {
                                Type = x.Type,
                                Name = concreteName,
                                IsCanonTypeInstance = hasCanonArg && concreteIsCanonOrDef,
                                IsTypeDef = !hasCanonArg && canonName.Contains("__Canon"),
                                IsMethodsViaCanon = false,
                                IsTypeViaCanon = false,
                                MethodSize = 0,
                                TypeSize = x.Size,
                            };
                        }), x => x.Name)
                        .ToArray();

                // These are types that have been erased/only have method footprint (boxed_..., static classes, or certain generic instantiations).
                intermediateStats =
                    intermediateStats.Concat(
                        filteredConverterTypeStats.Select(x => x.Type)
                            .Concat(filteredConverterMethodStats.Select(x => x.Value.DeclaringType))
                            .Select(x => GetCanonTypeName(x, out _))
                            .Except(intermediateStats.Select(x => GetCanonTypeName(x.Type, out _)))
                            .Select(name =>
                            {
                                var stats = filteredConverterMethodStats[name];
                                var canonName = GetCanonTypeName(stats.DeclaringType, out var hasCanonArg);
                                var genericTypeParams = stats.DeclaringType.IsGenericInstance
                                    ? ((GenericInstanceType)stats.DeclaringType).GenericArguments.Count
                                    : stats.DeclaringType.GenericParameters.Count;
                                var staticCanonName = stats.DeclaringType.Name;
                                if (genericTypeParams > 0)
                                    staticCanonName += Enumerable.Range(0, genericTypeParams).Aggregate(" <", (s, _) => s + "__Canon" + ", ")[..^2] + ">";
                                var concreteName = GetConcreteTypeName(stats.DeclaringType, out var concreteIsCanonOrDef);
                                var canonType = intermediateStats.FirstOrDefault(x => x.Name == staticCanonName);
                                return new
                                {
                                    Type = stats.DeclaringType,
                                    Name = concreteName,
                                    IsCanonTypeInstance = hasCanonArg && concreteIsCanonOrDef,
                                    IsTypeDef = !hasCanonArg && canonName.Contains("__Canon"),
                                    IsMethodsViaCanon = false,
                                    IsTypeViaCanon = canonType is not null,
                                    MethodSize = stats.Size,
                                    TypeSize = canonType?.TypeSize ?? 0,
                                };
                            })).ToArray();

                var filteredConverterTotalStats =
                    intermediateStats
                        .GroupBy(x => GetCanonTypeName(x.Type, out _))
                        .Select(x =>
                        {
                            // There are cases where a canon type only exists as a shared canon, so we add one with AreCanonMethods=false in such a case.
                            if (x.All(x => x.IsMethodsViaCanon && x.MethodSize > 0))
                            {
                                var canonMethod = filteredConverterMethodStats[GetCanonTypeName(x.First().Type, out _)];
                                var canonMethodName = GetCanonTypeName(canonMethod.DeclaringType, out var hasCanonArg);
                                var canonConcreteName = GetConcreteTypeName(canonMethod.DeclaringType, out var concreteIsCanonOrDef);
                                return x.Append(new
                                {
                                    Type = canonMethod.DeclaringType,
                                    Name = canonConcreteName,
                                    IsCanonTypeInstance = hasCanonArg && concreteIsCanonOrDef,
                                    IsTypeDef = !hasCanonArg && canonMethodName.Contains("__Canon"),
                                    IsMethodsViaCanon = false,
                                    IsTypeViaCanon = false,
                                    MethodSize = canonMethod.Size,
                                    TypeSize = 0,
                                });
                            }

                            // There are cases where we have both def and canon, only attribute methods to canon in such a case, unless canon has no method size.
                            if (x.Any(x => x.IsCanonTypeInstance && x.MethodSize > 0) && x.Any(x => x.IsTypeDef && x.MethodSize > 0))
                            {
                                return x
                                    .Select(x => x.IsTypeDef ? x with { MethodSize = 0 } : x);
                            }

                            return x.AsEnumerable();
                        }).SelectMany(x => x)
                        .Select(x =>
                        {
                            var typeSize = x.IsTypeViaCanon ? 0 : x.TypeSize;
                            return new
                            {
                                x.Type,
                                x.Name,
                                x.IsCanonTypeInstance,
                                x.IsTypeDef,
                                x.IsMethodsViaCanon,
                                x.IsTypeViaCanon,
                                x.MethodSize,
                                x.TypeSize,
                                TotalSize = x.IsMethodsViaCanon ? typeSize : x.MethodSize + typeSize
                            };
                        })
                    .OrderByDescending(x => x.TotalSize)
                    .ToArray();

                var originalSum = filteredConverterMethods.Sum(x => x.Size + x.EhInfoSize + x.GcInfoSize) + filteredConverterTypeStats.Sum(x => x.Size);
                var actualSum = filteredConverterTotalStats.Sum(x => x.TotalSize);
                if (originalSum != actualSum)
                    throw new InvalidOperationException($"Total size of stats is diverging after combining methods and types, actual: {actualSum}, expected: {originalSum}");

                Console.WriteLine("<details>");
                Console.WriteLine($"<summary>Converter Namespace Type and Method Size {actualSum:n0}</summary>");
                Console.WriteLine();
                Console.WriteLine("<br>");
                Console.WriteLine();
                Console.WriteLine("| Name | Type Size | Method Size | Total Size |");
                Console.WriteLine("| --- | --- | --- | --- |");
                foreach (var m in filteredConverterTotalStats)
                {
                    var name = m.Name.Replace("`", "\\`")
                        .Replace("<", "&#60;")
                        .Replace(">", "&#62;")
                        .Replace("|", "\\|");

                    var msize = m.MethodSize.ToString("n0");
                    var totalSize = m.TotalSize.ToString("n0");
                    if (m.IsMethodsViaCanon)
                    {
                        msize = $"canon: {m.MethodSize:n0}";
                    }
                    else if (m.MethodSize is 0)
                        msize = "none";

                    var tsize = m.TypeSize.ToString("n0");
                    if (m.IsTypeViaCanon)
                        tsize = "canon: " + m.TypeSize;
                    else if (m.TypeSize is 0)
                        tsize = "none";

                    Console.WriteLine($"| {name} | {tsize} | {msize} | {totalSize} |");
                }
                Console.WriteLine();
                Console.WriteLine("</details>");


                Console.WriteLine();
            }

            string GetConcreteTypeName(TypeReference type, out bool isCanon)
            {
                var name = (type.DeclaringType is { } t ? t.Name + "." : null) + type.Name;
                isCanon = false;
                if (type.IsGenericInstance)
                {
                    var canon = true;
                    name += ((GenericInstanceType)type).GenericArguments.Aggregate(" <",
                        (s, arg) =>
                        {
                            if (canon)
                                canon = arg.Name == "__Canon";
                            return s + GetConcreteTypeName(arg, out _) + ", ";
                        })[..^2] + ">";
                    isCanon = canon;
                }
                else if (type.Name.Contains("`"))
                    name += " (def)";

                return name;
            }

            string GetCanonTypeName(TypeReference type, out bool hasCanonArg)
            {
                hasCanonArg = false;
                var name = (type.DeclaringType is { } t ? t.Name + "." : null) + type.Name;
                if (type.IsGenericInstance)
                {
                    var canon = hasCanonArg;
                    name += ((GenericInstanceType)type).GenericArguments.Aggregate(" <",
                        (s, arg) =>
                        {
                            var ret = s;
                            if (arg.IsValueType)
                                ret += GetCanonTypeName(arg, out var _);
                            else
                            {
                                canon = true;
                                ret += "__Canon";
                            }

                            return ret + ", ";
                        })[..^2] + ">";
                    hasCanonArg = canon;
                }
                else if (type.Name.Contains("`"))
                    // We gen it but it doesn't exist as an arg.
                    name += type.GenericParameters.Aggregate(" <", (s, _) => s + "__Canon" + ", ")[..^2] + ">";

                return name;
            }
        }

        public static IEnumerable<TypeStats> GetTypes(MethodDefinition types)
        {
            types.Body.SimplifyMacros();
            var il = types.Body.Instructions;
            for (var i = 0; i + 2 < il.Count; i += 2)
            {
                var type = (TypeReference)il[i + 0].Operand;
                var size = (int)il[i + 1].Operand;
                yield return new TypeStats
                {
                    Type = type,
                    Size = size
                };
            }
        }

        public static IEnumerable<MethodStats> GetMethods(MethodDefinition methods)
        {
            methods.Body.SimplifyMacros();
            var il = methods.Body.Instructions;
            for (var i = 0; i + 4 < il.Count; i += 4)
            {
                var method = (MethodReference)il[i + 0].Operand;
                var size = (int)il[i + 1].Operand;
                var gcInfoSize = (int)il[i + 2].Operand;
                var ehInfoSize = (int)il[i + 3].Operand;
                yield return new MethodStats
                {
                    Method = method,
                    Size = size,
                    GcInfoSize = gcInfoSize,
                    EhInfoSize = ehInfoSize
                };
            }
        }

        public static IEnumerable<BlobStats> GetBlobs(MethodDefinition blobs)
        {
            blobs.Body.SimplifyMacros();
            var il = blobs.Body.Instructions;
            for (var i = 0; i + 2 < il.Count; i += 2)
            {
                var name = (string)il[i + 0].Operand;
                var size = (int)il[i + 1].Operand;
                yield return new BlobStats
                {
                    Name = name,
                    Size = size
                };
            }
        }
    }

    public class TypeStats
    {
        public string MethodName { get; set; }
        public TypeReference Type { get; set; }
        public int Size { get; set; }
    }

    public class MethodStats
    {
        public MethodReference Method { get; set; }
        public int Size { get; set; }
        public int GcInfoSize { get; set; }
        public int EhInfoSize { get; set; }
    }

    public class BlobStats
    {
        public string Name { get; set; }
        public int Size { get; set; }
    }
}

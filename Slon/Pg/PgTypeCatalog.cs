using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg;

// TODO This should replace the KVPair maybe?
sealed class PgTypeInfo
{

    //
    // internal override string GetPartialNameWithFacets(int typeModifier)
    // {
    //     var facets = GetFacets(typeModifier);
    //     if (facets == PostgresFacets.None)
    //         return Name;
    //
    //     return Name switch
    //     {
    //         // Special case for time, timestamp, timestamptz and timetz where the facet is embedded in the middle
    //         "timestamp without time zone" => $"timestamp{facets} without time zone",
    //         "time without time zone"      => $"time{facets} without time zone",
    //         "timestamp with time zone"    => $"timestamp{facets} with time zone",
    //         "time with time zone"         => $"time{facets} with time zone",
    //
    //         // We normalize character(1) to character - they mean the same
    //         "character" when facets.Size == 1 => "character",
    //
    //         _                             => $"{Name}{facets}"
    //     };
    // }
    //
    // internal override PostgresFacets GetFacets(int typeModifier)
    // {
    //     if (typeModifier == -1)
    //         return PostgresFacets.None;
    //
    //     switch (Name)
    //     {
    //     case "character":
    //         return new PostgresFacets(typeModifier - 4, null, null);
    //     case "character varying":
    //         return new PostgresFacets(typeModifier - 4, null, null);  // Max length
    //     case "numeric":
    //     case "decimal":
    //         // See https://stackoverflow.com/questions/3350148/where-are-numeric-precision-and-scale-for-a-field-found-in-the-pg-catalog-tables
    //         var precision = ((typeModifier - 4) >> 16) & 65535;
    //         var scale = (typeModifier - 4) & 65535;
    //         return new PostgresFacets(null, precision, scale == 0 ? (int?)null : scale);
    //     case "timestamp without time zone":
    //     case "time without time zone":
    //     case "interval":
    //         precision = typeModifier & 0xFFFF;
    //         return new PostgresFacets(null, precision, null);
    //     case "timestamp with time zone":
    //         precision = typeModifier & 0xFFFF;
    //         return new PostgresFacets(null, precision, null);
    //     case "time with time zone":
    //         precision = typeModifier & 0xFFFF;
    //         return new PostgresFacets(null, precision, null);
    //     case "bit":
    //     case "bit varying":
    //         return new PostgresFacets(typeModifier, null, null);
    //     default:
    //         return PostgresFacets.None;
    //     }
    // }

}

sealed partial class PgTypeCatalog
{
    // Important keys are represented by their underlying primitives for slightly faster lookups.
    // TODO this can use frozendicts for the 7.0 TFM.
    readonly Dictionary<string, PgType> _typesByDataTypeName = new();
    readonly Dictionary<uint, PgType>? _typesByOid;
    readonly Dictionary<uint, DataTypeName>? _dataTypeNameLookup;
    // This one and future additions for range/multirange serve purely as a lookup cache for inverse relations like element type -> array type etc.
    ConcurrentDictionary<PgTypeId, PgType>? _arrayTypesByElementId;

    PgTypeCatalog(IEnumerable<PgType> types)
    {
        foreach (var t in types)
        {
            if (!t.IsPortable)
                throw new ArgumentException($"Type '{t}' is not portable, all types passed to this constructor must be portable.");

            _typesByDataTypeName.Add((string)t.Identifier.DataTypeName, t);
        }
    }

    public PgTypeCatalog(IEnumerable<KeyValuePair<DataTypeName,PgType>> namesAndTypes)
    {
        var typesByOid = _typesByOid = new();
        var dataTypeNameLookup =  _dataTypeNameLookup = new();
        foreach (var kv in namesAndTypes)
        {
            var dataTypeName = kv.Key;
            var t = kv.Value;
            if (t.IsPortable)
                throw new ArgumentException($"Type '{t}' is portable, all types passed to this constructor must not be portable.");

            _typesByDataTypeName.Add((string)dataTypeName, t);
            if (typesByOid is not null && dataTypeNameLookup is not null)
            {
                typesByOid.Add((uint)t.Identifier.Oid, t);
                dataTypeNameLookup.Add((uint)t.Identifier.Oid, dataTypeName);
            }
        }
    }

    public IReadOnlyCollection<PgType> Types => _typesByDataTypeName.Values;

    public PgType GetPgType(PgTypeId pgTypeId)
    {
        ThrowIfPortableAndOidArgument(pgTypeId);
        if (pgTypeId.IsOid)
            return _typesByOid[(uint)pgTypeId.Oid];

        return _typesByDataTypeName[(string)pgTypeId.DataTypeName];
    }

    internal PgTypeCatalog CreatePortableCatalog(IEnumerable<PgType> types) => new(types);

    internal PgTypeCatalog ToPortableCatalog()
    {
        if (IsPortable)
            return new PgTypeCatalog(Types);

        return new PgTypeCatalog(GeneratePortableTypes());

        IEnumerable<PgType> GeneratePortableTypes()
        {
            foreach (var t in Types)
                yield return ToPortableType(t);
        }
    }

    PgType ToPortableType(PgType pgType)
    {
        if (pgType.IsPortable)
            return pgType;

        var portableType = pgType.Kind switch
        {
            PgKind.Base => pgType with { Identifier = GetDataTypeNameCore(pgType.Identifier) },
            PgKind.Array array => pgType with
            {
                Identifier = GetDataTypeNameCore(pgType.Identifier),
                Kind = array with
                {
                    ElementType = array.ElementType with
                    {
                        Identifier = GetDataTypeNameCore(array.ElementType.Identifier)
                    }
                }
            },
            PgKind.Range range => pgType with
            {
                Identifier = GetDataTypeNameCore(pgType.Identifier),
                Kind = range with
                {
                    ElementType = range.ElementType with
                    {
                        Identifier = GetDataTypeNameCore(range.ElementType.Identifier)
                    }
                }
            },
            PgKind.MultiRange multiRange => pgType with
            {
                Identifier = GetDataTypeNameCore(pgType.Identifier),
                Kind = multiRange with
                {
                    RangeType = multiRange.RangeType with
                    {
                        Identifier = GetDataTypeNameCore(multiRange.RangeType.Identifier)
                    }
                }
            },
            PgKind.Enum => pgType with { Identifier = GetDataTypeNameCore(pgType.Identifier) },
            PgKind.Composite composite => pgType with
            {
                Identifier = GetDataTypeNameCore(pgType.Identifier),
                Kind = ToPortableComposite(composite)
            },
        PgKind.Pseudo => pgType with { Identifier = GetDataTypeNameCore(pgType.Identifier), },
            PgKind.Domain domain => pgType with
            {
                Identifier = GetDataTypeNameCore(pgType.Identifier),
                Kind = domain with
                {
                    UnderlyingType = domain.UnderlyingType with
                    {
                        Identifier = GetDataTypeNameCore(domain.UnderlyingType.Identifier)
                    }
                }
            },
            var v => throw new ArgumentOutOfRangeException(nameof(pgType.Kind), v, null)
        };
        DebugShim.Assert(portableType.IsPortable);
        return portableType;
    }

    PgKind.Composite ToPortableComposite(PgKind.Composite composite)
    {
        var builder = ImmutableArray.CreateBuilder<Field>(composite.Fields.Length);
        foreach (var field in composite.Fields)
            builder.Add(field with { PgTypeId = GetDataTypeNameCore(field.PgTypeId) });

        return composite with
        {
            Fields = builder.MoveToImmutable()
        };
    }

    [MemberNotNull(nameof(_typesByOid))]
    void ThrowIfPortable()
    {
        if (_typesByOid is null)
            throw new InvalidOperationException("Cannot return oid information for this type catalog.");

        DebugShim.Assert(_typesByOid is not null);
    }

    [MemberNotNull(nameof(_typesByOid))]
    void ThrowIfPortableAndOidArgument(PgTypeId pgTypeId)
    {
        if (_typesByOid is null)
        {
            if (pgTypeId.IsOid)
                throw new InvalidOperationException("Cannot lookup types via oid type ids for this type catalog.");
        }
        DebugShim.Assert(_typesByOid is not null);
    }

    Oid GetOidCore(PgTypeId pgTypeId, bool validateIfOid = true)
    {
        ThrowIfPortable();
        if (pgTypeId.IsOid)
        {
            if (validateIfOid)
                _ = _typesByOid[(uint)pgTypeId.Oid];
            return pgTypeId.Oid;
        }

        return _typesByDataTypeName[(string)pgTypeId.DataTypeName].Identifier.Oid;
    }

    DataTypeName GetDataTypeNameCore(PgTypeId pgTypeId, bool validateIfDataTypeName = true)
    {
        ThrowIfPortableAndOidArgument(pgTypeId);
        if (pgTypeId.IsDataTypeName)
        {
            if (validateIfDataTypeName)
                _ = _typesByDataTypeName[(string)pgTypeId.DataTypeName];
            return pgTypeId.DataTypeName;
        }

        if (_dataTypeNameLookup is not null)
            return _dataTypeNameLookup[(uint)pgTypeId.Oid];

        return _typesByOid[(uint)pgTypeId.Oid].Identifier.DataTypeName;
    }

    /// Returns whether this type catalog can be used to lookup by oid and whether any returned types are portable by default.
    public bool IsPortable => _typesByOid is null;

    public Oid GetOid(PgTypeId pgTypeId) => GetOidCore(pgTypeId, validateIfOid: true);

    public Oid GetElementOid(PgTypeId arrayTypeId)
    {
        ThrowIfPortable();

        var type = GetPgType(arrayTypeId);
        if (type.Kind is not PgKind.Array arrayKind)
            throw new InvalidOperationException("Type is not a kind of Array.");

        return GetOid(arrayKind.ElementType.Identifier);
    }

    public Oid GetArrayOid(PgTypeId elementTypeId)
    {
        ThrowIfPortable();

        if ((_arrayTypesByElementId ??= new()).TryGetValue(elementTypeId, out var cached))
            return GetOidCore(cached.Identifier, validateIfOid: false);

        // Map it to oid as we have stored non-portable types which must be compared against.
        var elementOid = GetOidCore(elementTypeId, validateIfOid: false);
        foreach (var t in Types)
            if (t.Kind is PgKind.Array array && array.ElementType.Identifier.Oid == elementOid)
            {
                // We need to be able to store both the portable and oid version for the cache to work.
                _arrayTypesByElementId[elementTypeId] = t;
                return array.ElementType.Identifier.Oid;
            }

        throw new KeyNotFoundException();
    }

    public DataTypeName GetDataTypeName(string name)
    {
        if (!TryGetIdentifiers(name, out _, out var dataTypeName))
            throw new KeyNotFoundException();

        return dataTypeName;
    }

    public DataTypeName GetDataTypeName(PgTypeId pgTypeId) => GetDataTypeNameCore(pgTypeId, validateIfDataTypeName: true);

    public DataTypeName GetElementDataTypeName(PgTypeId arrayTypeId)
    {
        ThrowIfPortableAndOidArgument(arrayTypeId);
        var type = GetPgType(arrayTypeId);
        if (type.Kind is not PgKind.Array arrayKind)
            throw new InvalidOperationException("Type is not a kind of Array.");

        return GetDataTypeName(arrayKind.ElementType.Identifier);
    }

    public DataTypeName GetArrayDataTypeName(PgTypeId elementTypeId)
    {
        ThrowIfPortableAndOidArgument(elementTypeId);
        if ((_arrayTypesByElementId ??= new()).TryGetValue(elementTypeId, out var cached))
            return GetDataTypeNameCore(cached.Identifier, validateIfDataTypeName: false);

        // Map it to oid as we have stored non-portable types which must be compared against.
        var elementOid = GetOidCore(elementTypeId, validateIfOid: false);
        foreach (var t in Types)
            if (t.Kind is PgKind.Array array && array.ElementType.Identifier == elementOid)
            {
                // We need to be able to store both the portable and oid version for the cache to work.
                _arrayTypesByElementId[elementTypeId] = t;
                return GetDataTypeNameCore(t.Identifier, validateIfDataTypeName: false);
            }

        throw new KeyNotFoundException();
    }

    public bool TryGetMultiRangeIdentifiers(string rangeDataTypeName, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
    {
        // TODO
        throw new NotImplementedException();
        canonicalTypeId = default;
        dataTypeName = default;
        return false;
    }

    public bool TryGetArrayIdentifiers(string elementDataTypeName, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
    {
        // TODO
        throw new NotImplementedException();
        canonicalTypeId = default;
        dataTypeName = default;
        return false;
    }

    public bool TryGetIdentifiers(string name, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
    {
        var hasSchema = name.IndexOf('.') != -1;
        if (hasSchema && _typesByDataTypeName.TryGetValue(name, out var type))
        {
            canonicalTypeId = type.Identifier;
            dataTypeName = new DataTypeName(name);
            return true;
        }
        // TODO
        throw new NotImplementedException();
        canonicalTypeId = default;
        dataTypeName = default;
        return false;
    }
}

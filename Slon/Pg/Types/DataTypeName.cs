using System;

namespace Slon.Pg.Types;

readonly record struct DataTypeName
{
    // This is the maximum length of names in an unmodified Postgres installation.
    // We need to respect this to get to valid names when deriving them (for multirange/arrays etc)
    const int NAMEDATALEN = 64 - 1; // Minus null terminator.

    readonly string _value;

    DataTypeName(string fullyQualifiedDataTypeName, bool validated)
    {
        if (!validated)
        {
            var schemaEndIndex = fullyQualifiedDataTypeName.LastIndexOf('.');
            if (schemaEndIndex == -1)
                throw new ArgumentException("Given value does not contain a schema.", nameof(fullyQualifiedDataTypeName));

            var typeNameLength = fullyQualifiedDataTypeName.Length - schemaEndIndex + 1;
            if (typeNameLength > NAMEDATALEN)
                if (fullyQualifiedDataTypeName.EndsWith("[]", StringComparison.Ordinal) && typeNameLength == NAMEDATALEN + "[]".Length - "_".Length)
                    throw new ArgumentException($"Name is too long and would be truncated to: {fullyQualifiedDataTypeName.Substring(0, fullyQualifiedDataTypeName.Length - typeNameLength + NAMEDATALEN)}");
        }

        _value = fullyQualifiedDataTypeName;
    }

    public DataTypeName(string fullyQualifiedDataTypeName)
        :this(fullyQualifiedDataTypeName, validated: false) {}

    internal static DataTypeName ValidatedName(string fullyQualifiedDataTypeName)
        => new(fullyQualifiedDataTypeName, validated: true);

    public string Schema => ValueOrThrowIfDefault().Substring(0, _value.IndexOf('.'));
    public string UnqualifiedName => ValueOrThrowIfDefault().Substring(_value.IndexOf('.') + 1);
    public string UnqualifiedDisplayName => ToDisplayName(UnqualifiedName);

    // Includes schema unless it's pg_catalog.
    public string DisplayName =>
        ValueOrThrowIfDefault().StartsWith("pg_catalog", StringComparison.Ordinal)
            ? ToDisplayName(UnqualifiedName)
            : Schema + ToDisplayName(UnqualifiedName);

    public string Value => ValueOrThrowIfDefault();

    public static explicit operator string(DataTypeName value) => value.Value;

    internal bool IsDefault => _value is null;

    string ValueOrThrowIfDefault()
    {
        if (_value is not null)
            return _value;

        return Throw();

        string Throw() => throw new InvalidOperationException($"This operation cannot be performed on a default instance of {nameof(DataTypeName)}.");
    }

    internal static DataTypeName CreateFullyQualifiedName(string dataTypeName)
        => dataTypeName.IndexOf('.') != -1 ? new(dataTypeName) : new("pg_catalog." + dataTypeName);

    // Static transform as defined by https://www.postgresql.org/docs/current/sql-createtype.html#SQL-CREATETYPE-ARRAY
    public DataTypeName ToArrayName()
    {
        if (_value.StartsWith("_", StringComparison.Ordinal) || _value.EndsWith("[]", StringComparison.Ordinal))
            return this;

        if (_value.Length + "_".Length > NAMEDATALEN)
            return new("_" + _value.Substring(0, NAMEDATALEN - "_".Length));

        return new("_" + _value);
    }

    // Static transform as defined by https://www.postgresql.org/docs/current/sql-createtype.html#SQL-CREATETYPE-ARRAY
    // Manual testing confirmed it's only the first occurence of 'range' that gets replaced.
    public DataTypeName ToMultiRangeName()
    {
        if (_value.IndexOf("multirange", StringComparison.Ordinal) != -1)
            return this;

        var rangeIndex = _value.IndexOf("range", StringComparison.Ordinal);
        if (rangeIndex != -1)
        {
            var str = _value.Substring(0, rangeIndex) + "multirange" + _value.Substring(rangeIndex + "range".Length);

            if (_value.Length + "multi".Length > NAMEDATALEN)
                return new(str.Substring(0, NAMEDATALEN - "multi".Length));

            return new(str);
        }

        if (_value.Length + "_multirange".Length > NAMEDATALEN)
            return new(_value.Substring(0, NAMEDATALEN - "_multirange".Length) + "_multirange");

        return new(_value + "_multirange");
    }

    // The type names stored in a DataTypeName are usually the actual typname from the pg_type column.
    // There are some canonical aliases defined in the SQL standard which we take into account.
    // Additionally array types have a '_' prefix while for readability their element type should be postfixed with '[]'.
    // See the table for all the aliases https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
    // Alternatively the source lives at https://github.com/postgres/postgres/blob/c8e1ba736b2b9e8c98d37a5b77c4ed31baf94147/src/backend/utils/adt/format_type.c#L186
    static string ToDisplayName(string unqualifiedName)
    {
        var baseTypeName = unqualifiedName;
        var prefixedArrayType = unqualifiedName.IndexOf('_') == 0;
        var postfixedArrayType = unqualifiedName.EndsWith("[]", StringComparison.Ordinal);
        if (prefixedArrayType)
            baseTypeName = baseTypeName.Substring(1);
        else if (postfixedArrayType)
            baseTypeName = baseTypeName.Substring(0, baseTypeName.Length - 2);

        var mappedBaseType = baseTypeName switch
        {
            "bool"        => "boolean",
            "bpchar"      => "character",
            "decimal"     => "numeric",
            "float4"      => "real",
            "float8"      => "double precision",
            "int2"        => "smallint",
            "int4"        => "integer",
            "int8"        => "bigint",
            "time"        => "time without time zone",
            "timestamp"   => "timestamp without time zone",
            "timetz"      => "time with time zone",
            "timestamptz" => "timestamp with time zone",
            "varbit"      => "bit varying",
            "varchar"     => "character varying",
            _             => unqualifiedName
        };

        if (prefixedArrayType || postfixedArrayType)
            return mappedBaseType + "[]";

        return mappedBaseType;
    }
}



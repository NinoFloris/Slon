using System;
using System.Diagnostics.CodeAnalysis;
using Npgsql.Pipelines.Protocol;
using Npgsql.Pipelines.Protocol.Pg;
using Npgsql.Pipelines.Protocol.PgV3;

namespace Npgsql.Pipelines.Shared;

readonly struct CacheableStatement
{
    readonly string _statementText;
    readonly PgV3Statement _statement;

    public CacheableStatement(string statementText, PgV3Statement statement)
    {
        _statementText = statementText;
        _statement = statement;
    }

    public Guid Id => _statement.Id;
    public PreparationKind Kind => _statement.Kind;

    public bool TryGetValue(string statementText, PgTypeIdView parameterTypes, [NotNullWhen(true)] out Statement? value)
    {
        if (IsDefault)
            throw new InvalidOperationException($"This operation cannot be performed on a default instance of {nameof(CacheableStatement)}.");

        var cachedStatement = _statement;

        if (cachedStatement.IsInvalid)
        {
            value = null;
            return false;
        }

        var cachedParameterNames = cachedStatement.ParameterTypes;
        if (!ReferenceEquals(statementText, _statementText) || cachedParameterNames.Length != parameterTypes.Length)
        {
            value = null;
            return false;
        }

        var i = 0;
        foreach (var typeId in parameterTypes)
            if (!cachedParameterNames[i++].Equals(typeId))
            {
                value = null;
                return false;
            }

        // Parameters match with the cached statement.
        value = cachedStatement;
        return true;
    }

    public bool IsDefault => _statementText is null || _statement is null;
}

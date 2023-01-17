using System;
using System.Collections.Concurrent;
using Slon.Protocol.Pg;
using Slon.Protocol.PgV3;

namespace Slon;

sealed class StatementTracker
{
    readonly int _autoPrepareMinimumUses;
    readonly ConcurrentDictionary<Guid, PgV3Statement> _statements = new();
    readonly ConcurrentDictionary<string, PgV3Statement> _statementsBySql = new();

    public StatementTracker(int autoPrepareMinimumUses)
    {
        _autoPrepareMinimumUses = autoPrepareMinimumUses;
    }

    public PgV3Statement? Add(PgV3Statement statement)
    {
        var result = _statements.GetOrAdd(statement.Id, statement);
        if (result.IsComplete)
            return result;

        var uses = result.IncrementUses();
        if (uses < _autoPrepareMinimumUses)
            return null;

        return result;
    }

    public PgV3Statement? Lookup(string statementText, PgTypeIdView parameterTypeNames)
    {
        if (!_statementsBySql.TryGetValue(statementText, out var statement))
            return null;

        // TODO allow for multiple statements with differing parameter types to be cached.
        var i = 0;
        foreach (var dataTypeName in parameterTypeNames)
            if (!statement.ParameterTypes[i++].Equals(dataTypeName))
                return null;

        if (statement.IsInvalid)
        {
            // TODO remove statement.
        }

        if (statement.IsComplete)
            return statement;

        var uses = statement.IncrementUses();
        if (uses < _autoPrepareMinimumUses)
            return null;

        return statement;
    }

    public PgV3Statement? Lookup(Guid id)
    {
        if (!_statements.TryGetValue(id, out var statement))
            return null;

        return statement;
    }
}

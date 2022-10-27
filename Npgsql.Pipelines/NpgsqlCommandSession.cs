using System;
using Npgsql.Pipelines.Protocol;
using Npgsql.Pipelines.Protocol.PgV3.Commands;

namespace Npgsql.Pipelines;

class NpgsqlCommandSession: ICommandSession
{
    readonly NpgsqlDataSource _dataSource;
    Statement? _statement;

    public NpgsqlCommandSession(NpgsqlDataSource dataSource, ICommand.Values values)
    {
        _dataSource = dataSource;
        ExecutionFlags = values.ExecutionFlags;
        _statement = values.Statement;
    }

    public Statement? Statement => _statement;
    public ExecutionFlags ExecutionFlags { get; private set; }

    // Some mutability to support the janky multiplexing lifecycle.
    public void SetPreparing()
    {
        if (ExecutionFlags.HasPrepared())
            throw new InvalidOperationException("Cannot set preparing for an already prepared statement.");

        ExecutionFlags = (ExecutionFlags & ~ExecutionFlags.Unprepared) | ExecutionFlags.Preparing;
    }

    // TODO we need a version where we back out of completing (removing the identifier from the active list).
    public void CompletePreparation(Statement statement)
    {
        if (!statement.IsComplete)
            throw new ArgumentException("Statement is not completed", nameof(statement));

        if (Statement?.Id != statement.Id)
            throw new ArgumentException("Statement does not match the statement for this session.", nameof(statement));

        _statement = statement;
        // TODO shuttle completion back to the datasource statement tracker.
    }

    public void SubmitOutputParameters(ReadOnlyMemory<CommandParameter> parameters)
    {
        throw new NotImplementedException();
    }
}

using System;
using System.Collections.Generic;

namespace Npgsql.Pipelines.Protocol.Pg;

interface ICommandSession
{
    public Statement? Statement { get; }

    public IReadOnlyCollection<IParameterSession>? WritableParameters { get; }
    public void CloseWritableParameters();

    /// <summary>
    /// Invoked after the statement has been successfully prepared, providing backend information about the statement.
    /// </summary>
    /// <param name="statement"></param>
    public void CompletePreparation(Statement statement);
    public void CancelPreparation(Exception? ex);
}

readonly struct CommandExecution
{
    readonly ExecutionFlags _executionFlags;
    readonly CommandFlags _flags;
    readonly object? _sessionOrStatement;

    CommandExecution(ExecutionFlags executionFlags, CommandFlags flags, object? sessionOrStatement)
    {
        _executionFlags = executionFlags;
        _flags = flags;
        _sessionOrStatement = sessionOrStatement;
    }

    // TODO improve the api of this 'thing'.
    public (ExecutionFlags ExecutionFlags, CommandFlags Flags) TryGetSessionOrStatement(out ICommandSession? session, out Statement? statement)
    {
        if (_sessionOrStatement is Statement value)
        {
            statement = value;
            session = null;
            return (_executionFlags, _flags);
        }

        statement = null;
        session = (ICommandSession?)_sessionOrStatement;
        return (_executionFlags, _flags);
    }

    public static CommandExecution Create(ExecutionFlags executionFlags, CommandFlags flags) => new(executionFlags, flags, null);
    public static CommandExecution Create(ExecutionFlags executionFlags, CommandFlags flags, ICommandSession session)
    {
        var prepared = executionFlags.HasPrepared();
        if (!executionFlags.HasPreparing() && !prepared)
            throw new ArgumentException("Execution flags does not have Preparing or Prepared.", nameof(executionFlags));

        // We cannot check the inverse for Preparing because a connection can be preparing a previously completed statement.
        if (prepared && session.Statement?.IsComplete == false)
            throw new ArgumentException("Execution flags has Prepared but session.Statement is not complete.", nameof(executionFlags));

        return new(executionFlags, flags, session);
    }
    public static CommandExecution Create(ExecutionFlags executionFlags, CommandFlags flags, Statement statement)
    {
        if (!executionFlags.HasPrepared())
            throw new ArgumentException("Execution flags does not have Prepared.", nameof(executionFlags));

        if (!statement.IsComplete)
            throw new ArgumentException("Statement is not complete.", nameof(statement));

        return new(executionFlags, flags, statement);
    }
}

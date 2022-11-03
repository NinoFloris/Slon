using System;
using Npgsql.Pipelines.Protocol.PgV3.Commands;

namespace Npgsql.Pipelines.Protocol;

interface ICommandSession
{
    public Statement? Statement { get; }

    /// <summary>
    /// Invoked after the statement has been successfully prepared, providing backend information about the statement.
    /// </summary>
    /// <param name="statement"></param>
    /// <exception cref="System.InvalidOperationException">
    /// Thrown when <see cref="ICommandSession.Statement.IsCompleted">Statement.IsCompleted</see> is already true.
    /// </exception>
    public void CompletePreparation(Statement statement);
}

// TODO deliberate if Statement shouldn't just implement ICommandSession now that ExecutionFlags are moved here.
readonly struct CommandExecution
{
    readonly ExecutionFlags _executionFlags;
    readonly object? _sessionOrStatement;

    CommandExecution(ExecutionFlags executionFlags, object? sessionOrStatement)
    {
        _executionFlags = executionFlags;
        _sessionOrStatement = sessionOrStatement;
    }

    /// If ExecutionFlags.Prepared then statement is not null, if ExecutionFlags.Preparing session is not null, otherwise both are.
    public ExecutionFlags TryGetSessionOrStatement(out ICommandSession? session, out Statement? statement)
    {
        if (_executionFlags.HasPrepared())
        {
            statement = (Statement)_sessionOrStatement!;
            session = null;
            return _executionFlags;
        }

        statement = null;
        session = _sessionOrStatement as ICommandSession;
        return _executionFlags;
    }

    public static CommandExecution Create(ExecutionFlags executionFlags) => new(executionFlags, null);
    public static CommandExecution Create(ExecutionFlags executionFlags, ICommandSession session)
    {
        if (!executionFlags.HasPreparing())
            throw new ArgumentException("Execution flags should have Preparing.", nameof(session));

        return new(executionFlags, session);
    }
    public static CommandExecution Create(ExecutionFlags executionFlags, Statement statement)
    {
        if (!executionFlags.HasPrepared())
            throw new ArgumentException("Execution flags should have Prepared.", nameof(statement));

        return new(executionFlags, statement);
    }
}

// Used to link up CommandContexts constructed before a session is available with an actual session or statement later on.
interface ICommandExecutionProvider
{
    public CommandExecution Get(in CommandContext context);
}

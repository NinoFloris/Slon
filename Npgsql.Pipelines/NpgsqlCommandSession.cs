using System;
using System.Collections.Generic;
using Npgsql.Pipelines.Protocol;
using Npgsql.Pipelines.Protocol.Pg;

namespace Npgsql.Pipelines;

class NpgsqlCommandSession: ICommandSession
{
    readonly NpgsqlDataSource _dataSource;
    Statement? _statement;
    readonly ParameterContext? _parameterContext;
    List<IParameterSession>? _outputSessions;

    public NpgsqlCommandSession(NpgsqlDataSource dataSource, in IPgCommand.Values values)
    {
        _dataSource = dataSource;
        _statement = values.Statement;
        var parameterContext = values.Additional.ParameterContext;
        if (parameterContext.HasOutputSessions())
            _parameterContext = parameterContext;
    }

    public Statement? Statement => _statement;

    /// <summary>
    /// Contains sessions with a ParameterKind other than ParameterKind.Input
    /// </summary>
    public IReadOnlyCollection<IParameterSession>? WritableParameters
    {
        get
        {
            // TODO return a view instead.
            if (_parameterContext is null)
                return Array.Empty<IParameterSession>();

            if (_outputSessions is { } sessions)
                return sessions;

            var outputSessions = new List<IParameterSession>();
            foreach (var p in _parameterContext!.Value.Parameters.Span)
            {
                if (p.TryGetParameterSession(out var session))
                    outputSessions.Add(session);
            }

            return _outputSessions = outputSessions;
        }
    }

    public void CloseWritableParameters()
    {
        if (_parameterContext is not { } context)
            return;

        List<Exception>? exceptions = null;
        foreach (var session in WritableParameters!)
        {
            try
            {
                session.Close();
            }
            catch (Exception ex)
            {
                (exceptions ??= new()).Add(ex);
            }
        }

        context.Dispose();

        if (exceptions is not null)
            throw new AggregateException(exceptions);
    }

    public void CompletePreparation(Statement statement)
    {
        if (!statement.IsComplete)
            throw new ArgumentException("Statement is not completed", nameof(statement));

        if (Statement?.Id != statement.Id)
            throw new ArgumentException("Statement does not match the statement for this session.", nameof(statement));

        _statement = statement;
        // _dataSource.CompletePreparation(ICommand.Values  )
    }

    public void CancelPreparation(Exception? ex)
    {
        throw new NotImplementedException();
    }
}

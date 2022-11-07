using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol;
using Npgsql.Pipelines.Protocol.PgV3.Commands;
using Npgsql.Pipelines.Protocol.PgV3.Types;

namespace Npgsql.Pipelines;

static class CommandBehaviorExtensions
{
    public static ExecutionFlags ToExecutionFlags(this CommandBehavior commandBehavior)
    {
        // Remove any irrelevant flags and mask the rest of the range for ExecutionFlags so users can't leak through flags.
        const int allFlags = (int)CommandBehavior.CloseConnection * 2 - 1; // 2^6 - 1.
        return (ExecutionFlags)(commandBehavior & ~((CommandBehavior)int.MaxValue - allFlags | CommandBehavior.CloseConnection | CommandBehavior.SingleResult));
    }
}

// Implementation
public sealed partial class NpgsqlCommand: ICommand
{
    bool _preparationRequested;
    object? _dataSourceOrConnection;
    CommandType _commandType = CommandType.Text;
    TimeSpan _commandTimeout = NpgsqlDataSourceOptions.DefaultCommandTimeout;
    NpgsqlTransaction? _transaction;
    string? _userCommandText;
    bool _disposed;
    NpgsqlParameterCollection? _parameterCollection;

    void Constructor(string? commandText, NpgsqlConnection? conn, NpgsqlTransaction? transaction, NpgsqlDataSource? dataSource = null)
    {
        GC.SuppressFinalize(this);
        _userCommandText = commandText;
        _transaction = transaction;
        if (conn is not null)
        {
            _dataSourceOrConnection = conn;
            _commandTimeout = conn.DefaultCommandTimeout;
        }
        else if (dataSource is not null)
        {
            _dataSourceOrConnection = dataSource;
            _commandTimeout = dataSource.DefaultCommandTimeout;
        }
    }

    void SetCommandText(string? value)
    {
        _preparationRequested = false;
        _userCommandText = value;
    }

    Statement? GetDataSourceStatement()
    {
        // TODO consult the datasource statement tracker.
        return null;
    }

    ICommand.Values ICommand.GetValues(CommandParameters parameters, ExecutionFlags additionalFlags)
    {
        ImmutableArray<Parameter> parameterTypes = default;

        var statement = _preparationRequested ? Statement.CreateUnprepared(PreparationKind.Command, parameterTypes) : GetDataSourceStatement();
        var flags = ExecutionFlags.ErrorBarrier | statement switch
        {
            { IsComplete: true } => ExecutionFlags.Prepared,
            { } => ExecutionFlags.Preparing,
            _ => ExecutionFlags.Unprepared
        };

        return new()
        {
            CommandParameters = parameters,
            StatementText = _userCommandText ?? string.Empty,
            ExecutionFlags = flags | additionalFlags,
            Statement = statement,
            Timeout = _commandTimeout,
            State = TryGetDataSource(out var dataSource) ? dataSource : GetConnection().DbDataSource
        };
    }

    CommandExecution ICommand.CreateExecution(in ICommand.Values values)
    {
        return Core(values);
        // This is a static local function to assure StartExecution has only dependencies on clearly passed in state.
        // Any unexpected instance dependencies would undoubtedly cause fun races.
        static CommandExecution Core(in ICommand.Values values)
        {
            var flags = values.ExecutionFlags;
            DebugShim.Assert(values.State is NpgsqlDataSource);
            // We only allocate to facilitate preparation, which is rare during steady state operations.
            var session = flags.HasPreparing() ? new NpgsqlCommandSession((NpgsqlDataSource)values.State, values) : null;

            DebugShim.Assert(flags.HasUnprepared() || values.Statement is not null);

            // TODO if Statement implements ICommandSession this collapses to something even simpler.
            var commandExecution = flags switch
            {
                _ when flags.HasPrepared() => CommandExecution.Create(flags, values.Statement!),
                _ when session is not null => CommandExecution.Create(flags, session),
                _ => CommandExecution.Create(flags)
            };

            return commandExecution;
        }
    }

    static CommandParameters TransformParameters(NpgsqlParameterCollection? parameters)
    {
        ReadOnlyMemory<KeyValuePair<CommandParameter, IParameterWriter>> collection;
        int count;
        if (parameters is null || (count = parameters.Count) == 0)
            collection = new();
        else
        {
            var array=  ArrayPool<KeyValuePair<CommandParameter, IParameterWriter>>.Shared.Rent(count);
            var i = 0;
            foreach (var p in parameters.GetFastEnumerator())
            {
                // Start session, lookup type info, writer etc.
                var parameter = ToCommandParameter(p);
                array[i] = new(parameter, LookupWriter(parameter));
                i++;
            }

            collection = new(array, 0, count);
        }

        return new CommandParameters { Collection = collection };

        CommandParameter ToCommandParameter(KeyValuePair<string, object?> keyValuePair)
        {
            throw new NotImplementedException();
        }

        // Probably want this writer to be a normal class.
        IParameterWriter LookupWriter(CommandParameter commandParameter)
        {
            throw new NotImplementedException();
        }
    }

    bool ConnectionOpInProgress
        => TryGetConnection(out var connection) && connection.ConnectionOpInProgress;

    void ThrowIfDisposed()
    {
        if (_disposed)
            ThrowObjectDisposed();

        static void ThrowObjectDisposed() => throw new ObjectDisposedException(nameof(NpgsqlCommand));
    }

    bool TryGetConnection([NotNullWhen(true)]out NpgsqlConnection? connection)
    {
        connection = _dataSourceOrConnection as NpgsqlConnection;
        return connection is not null;
    }
    NpgsqlConnection GetConnection() => TryGetConnection(out var connection) ? connection : throw new NullReferenceException("Connection is null.");
    NpgsqlConnection.CommandWriter GetCommandWriter() => GetConnection().GetCommandWriter();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    bool TryGetDataSource([NotNullWhen(true)]out NpgsqlDataSource? connection)
    {
        connection = _dataSourceOrConnection as NpgsqlDataSource;
        return connection is not null;
    }

    // Only for DbConnection commands, throws for DbDataSource commands.
    bool HasCloseConnection(CommandBehavior behavior) => (behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection;
    void ThrowIfHasCloseConnection(CommandBehavior behavior)
    {
        if (HasCloseConnection(behavior))
            ThrowHasCloseConnection();

        void ThrowHasCloseConnection() => throw new ArgumentException($"Cannot pass {nameof(CommandBehavior.CloseConnection)} for a DbDataSource command, this is only valid when a command has a connection.");
    }

    NpgsqlDataReader ExecuteDataReader(CommandBehavior behavior)
    {
        ThrowIfDisposed();
        if (TryGetDataSource(out var dataSource))
        {
            ThrowIfHasCloseConnection(behavior);
            // Pick a connection and do the write ourselves, connectionless command execution for sync paths :)
            var slot = dataSource.Open(exclusiveUse: false, dataSource.DefaultConnectionTimeout);
            var command = dataSource.WriteCommand(slot, this, TransformParameters(_parameterCollection), behavior.ToExecutionFlags());
            return NpgsqlDataReader.Create(async: false, new ValueTask<CommandContextBatch>(command)).GetAwaiter().GetResult();
        }
        else
        {
            var command = GetCommandWriter().WriteCommand(allowPipelining: false, this, TransformParameters(_parameterCollection), behavior.ToExecutionFlags(), HasCloseConnection(behavior));
            return NpgsqlDataReader.Create(async: false, command).GetAwaiter().GetResult();
        }
    }

    // TODO would be interesting to prototype an overload taking a parametercollection, that would allow for entirely static/frozen DbDataSource commmands.
    ValueTask<NpgsqlDataReader> ExecuteDataReaderAsync(NpgsqlParameterCollection? parameters, CommandBehavior behavior, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (TryGetDataSource(out var dataSource))
        {
            ThrowIfHasCloseConnection(behavior);
            var command = dataSource.WriteMultiplexingCommand(this, TransformParameters(parameters ?? _parameterCollection), behavior.ToExecutionFlags(), cancellationToken);
            return NpgsqlDataReader.Create(async: true, command);
        }
        else
        {
            var command = GetCommandWriter().WriteCommand(allowPipelining: true, this, TransformParameters(parameters ?? _parameterCollection), behavior.ToExecutionFlags(), HasCloseConnection(behavior), cancellationToken);
            return NpgsqlDataReader.Create(async: true, command);
        }
    }

    async ValueTask DisposeCore(bool async)
    {
        if (_disposed)
            return;
        _disposed = true;
        // TODO
        await new ValueTask().ConfigureAwait(false);
    }
}

// Public surface & ADO.NET
public sealed partial class NpgsqlCommand: DbCommand
{
    public NpgsqlCommand() : this(null, null, null) {}
    public NpgsqlCommand(string? commandText) : this(commandText, null, null) {}
    public NpgsqlCommand(string? commandText, NpgsqlConnection? conn) : this(commandText, conn, null) {}
    public NpgsqlCommand(string? commandText, NpgsqlConnection? conn, NpgsqlTransaction? transaction)
        => Constructor(commandText, conn, transaction);

    internal NpgsqlCommand(string? commandText, NpgsqlDataSource dataSource)
        => Constructor(commandText, null, null, dataSource: dataSource);

    public override void Prepare()
    {
        ThrowIfDisposed();
        _preparationRequested = true;
    }

    [AllowNull]
    public override string CommandText
    {
        get => _userCommandText ?? string.Empty;
        set => SetCommandText(value);
    }
    // TODO, what time span should CommandTimeout cover? The first read or the entire pipeline + first read (unused today).
    public override int CommandTimeout {
        get => (int)_commandTimeout.TotalSeconds;
        set
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), "Cannot be zero or negative.");
            _commandTimeout = TimeSpan.FromSeconds(value);
        }
    }
    public override CommandType CommandType
    {
        get => _commandType;
        set
        {
            if (!EnumShim.IsDefined(value))
                throw new ArgumentOutOfRangeException();
            _commandType = value;
        }
    }

    /// <summary>
    /// Setting this property is ignored by Npgsql as its values are not respected.
    /// Gets or sets how command results are applied to the DataRow when used by the
    /// DbDataAdapter.Update(DataSet) method.
    /// </summary>
    /// <value>One of the <see cref="System.Data.UpdateRowSource"/> values.</value>
    public override UpdateRowSource UpdatedRowSource
    {
        get => UpdateRowSource.None;
        set { }
    }

    public new NpgsqlParameterCollection Parameters => _parameterCollection ??= new();

    /// <summary>
    /// Setting this property is ignored by Npgsql. PostgreSQL only supports a single transaction at a given time on
    /// a given connection, and all commands implicitly run inside the current transaction started via
    /// <see cref="NpgsqlConnection.BeginTransaction()"/>
    /// </summary>
    public new NpgsqlTransaction? Transaction { get => _transaction; set {} }

    public override bool DesignTimeVisible { get; set; }

    public override void Cancel()
    {
        // We can't throw in connectionless scenarios as dapper etc expect this method to work.
        if (ConnectionOpInProgress)
            return;

        // TODO We might be able to support it on arbitrary commands by creating protocol level support for it, not today :)
        if (!TryGetConnection(out var connection))
            return;

        connection.PerformUserCancellation();
    }

    public override int ExecuteNonQuery()
    {
        throw new System.NotImplementedException();
    }

    public override object? ExecuteScalar()
    {
        throw new System.NotImplementedException();
    }

    public new NpgsqlDataReader ExecuteReader()
        => ExecuteDataReader(CommandBehavior.Default);
    public new NpgsqlDataReader ExecuteReader(CommandBehavior behavior)
        => ExecuteDataReader(behavior);

    public new Task<NpgsqlDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
        => ExecuteDataReaderAsync(null, CommandBehavior.Default, cancellationToken).AsTask();
    public new Task<NpgsqlDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default)
        => ExecuteDataReaderAsync(null, behavior, cancellationToken).AsTask();

    public ValueTask<NpgsqlDataReader> ExecuteReaderAsync(NpgsqlParameterCollection? parameters, CancellationToken cancellationToken = default)
        => ExecuteDataReaderAsync(parameters, CommandBehavior.Default, cancellationToken);

    public ValueTask<NpgsqlDataReader> ExecuteReaderAsync(NpgsqlParameterCollection? parameters, CommandBehavior behavior, CancellationToken cancellationToken = default)
        => ExecuteDataReaderAsync(parameters, behavior, cancellationToken);

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        => ExecuteDataReader(behavior);
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        => await ExecuteDataReaderAsync(null, behavior, cancellationToken);

    protected override DbParameter CreateDbParameter() => NpgsqlDbParameter.Create();
    protected override DbConnection? DbConnection {
        get => _dataSourceOrConnection as NpgsqlConnection;
        set
        {
            ThrowIfDisposed();
            if (value is not NpgsqlConnection conn)
                throw new ArgumentException($"Value is not an instance of {nameof(NpgsqlConnection)}.", nameof(value));

            if (TryGetDataSource(out _))
                throw new InvalidOperationException("This is a DbDataSource command and cannot be assigned to connections.");

            if (ConnectionOpInProgress)
                throw new InvalidOperationException("An open data reader exists for this command.");

            _dataSourceOrConnection = conn;
        }
    }

    protected override DbParameterCollection DbParameterCollection => Parameters;
    protected override DbTransaction? DbTransaction { get => Transaction; set {} }

#if !NETSTANDARD2_0
    public override ValueTask DisposeAsync()
#else
    public ValueTask DisposeAsync()
#endif
        => DisposeCore(async: true);

    protected override void Dispose(bool disposing)
        => DisposeCore(false).GetAwaiter().GetResult();
}

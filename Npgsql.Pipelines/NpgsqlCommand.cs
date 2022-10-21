using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol;
using Npgsql.Pipelines.Protocol.PgV3;

namespace Npgsql.Pipelines;

// Implementation
public sealed partial class NpgsqlCommand: ICommandInfo
{
    bool _shouldPrepare;
    object? _dataSourceOrConnection;
    CommandKind _commandKind = CommandKind.Unprepared;
    CommandType _commandType = CommandType.Text;
    TimeSpan _commandTimeout = NpgsqlDataSourceOptions.DefaultCommandTimeout;
    NpgsqlTransaction? _transaction;
    string? _userCommandText;
    ValueTask<Operation>? _pendingConnectionOp;
    bool _disposed;

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

    CommandKind ICommandInfo.CommandKind => _commandKind;
    string? ICommandInfo.PreparedStatementName => _shouldPrepare ? Guid.NewGuid().ToString() : null;
    ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> ICommandInfo.Parameters => new();
    bool ICommandInfo.AppendErrorBarrier => true;
    string? ICommandInfo.CommandText => _userCommandText;

    bool ConnectionOpInProgress
        => _pendingConnectionOp?.IsCompleted == false || _pendingConnectionOp is { IsCompletedSuccessfully: true, Result.IsCompleted: false };

    void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(NpgsqlCommand));
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

    NpgsqlDataReader ExecuteDataReader(CommandBehavior behavior)
    {
        // ThrowIfDisposed();
        // if (TryGetDataSource(out var dataSource))
        // {
        //     // Pick a connection and do the write ourselves, connectionless command execution for sync paths :)
        //     var slot = dataSource.Open(exclusiveUse: false, dataSource.DefaultConnectionTimeout);
        //     var command = dataSource.WriteCommand(slot, this, behavior);
        //     return ReaderPool.Rent().Intialize(async: false, command, behavior, _commandTimeout).GetAwaiter().GetResult();
        // }
        // else
        // {
        //     var command = GetCommandWriter().WriteCommand(allowPipelining: false, this, behavior);
        //     // Store the active operation to know when it's completed.
        //     _pendingConnectionOp = command.GetProtocol();
        //     return ReaderPool.Rent().Intialize(async: false, command, behavior, _commandTimeout).GetAwaiter().GetResult();
        // }
        return null!;
    }

    ValueTask<NpgsqlDataReader> ExecuteDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (TryGetDataSource(out var dataSource))
        {
            var command = dataSource.WriteMultiplexingCommand(this, behavior, cancellationToken);
            return NpgsqlDataReader.Create(async: true, CommandBatch.Create(command), behavior, _commandTimeout, null, cancellationToken);
        }
        else
        {
            var command = GetCommandWriter().WriteCommand(allowPipelining: true, this, behavior, cancellationToken);
            // We store the last operation to know when it's completed, as a connection is at most a single
            // pipeline this is sufficient to know whether all previous commands were also completed.
            _pendingConnectionOp = command.GetProtocol();
            return NpgsqlDataReader.Create(async: true, CommandBatch.Create(command), behavior, _commandTimeout, null, cancellationToken);
        }
    }

    async ValueTask DisposeCore(bool async)
    {
        if (_disposed)
            return;
        _disposed = true;
        await new ValueTask();
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
        _shouldPrepare = true;
    }

    [AllowNull]
    public override string CommandText
    {
        get => _userCommandText ?? string.Empty;
        set => _userCommandText = value;
    }
    // TODO, what time span should CommandTimeout cover? The first read or the entire pipeline + first read (unusued today).
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

    // Literally not used, so it can stay in the ado partial.
    UpdateRowSource _updateRowSource;
    /// <summary>
    /// Gets or sets how command results are applied to the DataRow when used by the
    /// DbDataAdapter.Update(DataSet) method.
    /// </summary>
    /// <value>One of the <see cref="System.Data.UpdateRowSource"/> values.</value>
    public override UpdateRowSource UpdatedRowSource
    {
        get => _updateRowSource;
        set
        {
            if (!EnumShim.IsDefined(value))
                throw new ArgumentOutOfRangeException();
            _updateRowSource = value;
        }
    }

    /// <summary>
    /// This property is ignored by Npgsql. PostgreSQL only supports a single transaction at a given time on
    /// a given connection, and all commands implicitly run inside the current transaction started via
    /// <see cref="NpgsqlConnection.BeginTransaction()"/>
    /// </summary>
    public new NpgsqlTransaction? Transaction
    {
        get => (NpgsqlTransaction?)DbTransaction;
        set => DbTransaction = value;
    }

    public override bool DesignTimeVisible { get; set; }

    /// <summary>
    /// Creates a new instance of a <see cref="NpgsqlParameter"/> object.
    /// </summary>
    /// <returns>An <see cref="NpgsqlParameter"/> object.</returns>
    public new NpgsqlParameter CreateParameter() => new();

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
        => ExecuteDataReaderAsync(CommandBehavior.Default, cancellationToken).AsTask();
    public new Task<NpgsqlDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default)
        => ExecuteDataReaderAsync(behavior, cancellationToken).AsTask();

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        => ExecuteDataReader(behavior);
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        => await ExecuteDataReaderAsync(behavior, cancellationToken);

    protected override DbParameter CreateDbParameter() => CreateParameter();
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

    protected override DbParameterCollection DbParameterCollection => throw new NotImplementedException();
    protected override DbTransaction? DbTransaction
    {
        get => _transaction;
        set => _transaction = (NpgsqlTransaction?)value;
    }

#if !NETSTANDARD2_0
    public override ValueTask DisposeAsync()
#else
    public ValueTask DisposeAsync()
#endif
        => DisposeCore(async: true);

    protected override void Dispose(bool disposing)
        => DisposeCore(false).GetAwaiter().GetResult();
}

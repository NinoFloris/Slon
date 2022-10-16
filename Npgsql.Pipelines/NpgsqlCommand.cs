using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol;
using Npgsql.Pipelines.Protocol.PgV3;

namespace Npgsql.Pipelines;

public sealed class NpgsqlCommand: DbCommand, ICommandInfo
{
    static ObjectPool<NpgsqlDataReader> ReaderPool { get; } = new(pool =>
    {
        var returnAction = pool.Return;
        return () => new NpgsqlDataReader(returnAction);
    });

    bool _prepare;

    object? _dataSourceOrConnection;
    readonly bool _managedMultiplexing;
    CommandKind _commandKind;

    public NpgsqlCommand(NpgsqlConnection conn)
    {
        GC.SuppressFinalize(this);
        _dataSourceOrConnection = conn;
    }

    // Connectionless
    internal NpgsqlCommand(NpgsqlDataSource dataDataSource, bool managedMultiplexing = false)
    {
        GC.SuppressFinalize(this);
        _dataSourceOrConnection = dataDataSource;
        _managedMultiplexing = managedMultiplexing;
    }

    public override void Cancel()
    {
        throw new System.NotImplementedException();
    }

    public override int ExecuteNonQuery()
    {
        throw new System.NotImplementedException();
    }

    public override object? ExecuteScalar()
    {
        throw new System.NotImplementedException();
    }

    public override void Prepare()
    {
        _prepare = true;
    }

    public override string CommandText { get; set; }
    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }
    protected override DbConnection? DbConnection {
        get => _dataSourceOrConnection as NpgsqlConnection;
        set
        {
            if (value is not NpgsqlConnection conn)
                throw new ArgumentException($"Value is not an instance of {nameof(NpgsqlConnection)}.", nameof(value));

            _dataSourceOrConnection = conn;
        }
    }
    protected override DbParameterCollection DbParameterCollection { get; }
    protected override DbTransaction? DbTransaction { get; set; }
    public override bool DesignTimeVisible { get; set; }

    bool TryGetConnection([NotNullWhen(true)]out NpgsqlConnection? connection)
    {
        if (_dataSourceOrConnection is NpgsqlConnection conn)
        {
            connection = conn;
            return true;
        }

        connection = null;
        return false;
    }
    NpgsqlConnection GetConnection() => TryGetConnection(out var connection) ? connection : throw new NullReferenceException("Connection is null.");
    NpgsqlConnection.CommandWriter GetCommandWriter() => GetConnection().GetCommandWriter();

    bool TryGetDataSource([NotNullWhen(true)]out NpgsqlDataSource? connection)
    {
        if (_dataSourceOrConnection is NpgsqlDataSource conn)
        {
            connection = conn;
            return true;
        }

        connection = null;
        return false;
    }
    NpgsqlDataSource GetDataSource() => TryGetDataSource(out var dataSource) ? dataSource : throw new NullReferenceException("DbDataSource is null.");

    protected override System.Data.Common.DbParameter CreateDbParameter()
    {
        throw new System.NotImplementedException();
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        throw new System.NotImplementedException();
    }

    public new Task<NpgsqlDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
        => ExecuteDataReaderAsync(CommandBehavior.Default, cancellationToken).AsTask();

    public new Task<NpgsqlDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default)
        => ExecuteDataReaderAsync(behavior, cancellationToken).AsTask();

    ValueTask<NpgsqlDataReader> ExecuteDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (TryGetDataSource(out var dataSource))
        {
            if (_managedMultiplexing)
            {
                var completionPair = dataSource.WriteMultiplexingCommand(this, behavior, cancellationToken);
                return ReaderPool.Rent().IntializeAsync(completionPair, behavior, cancellationToken);
            }

            return UnmanagedMultiplexing(this, dataSource, behavior, cancellationToken);
        }
        else
        {
            var completionPair = GetCommandWriter().WriteCommand(this, behavior, cancellationToken);
            return ReaderPool.Rent().IntializeAsync(completionPair, behavior, cancellationToken);
        }

        static async ValueTask<NpgsqlDataReader> UnmanagedMultiplexing(NpgsqlCommand instance, NpgsqlDataSource dataSource, CommandBehavior behavior, CancellationToken cancellationToken)
        {
            // Pick a connection and do the write ourselves.
            var slot = await dataSource.OpenAsync(exclusiveUse: false, cancellationToken: cancellationToken);
            var completionPair = dataSource.WriteCommand(slot, instance, behavior, cancellationToken);
            return await ReaderPool.Rent().IntializeAsync(completionPair, behavior, cancellationToken);
        }
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        => await ExecuteDataReaderAsync(behavior, cancellationToken);

    CommandKind ICommandInfo.CommandKind => _commandKind;
    string? ICommandInfo.PreparedStatementName => _prepare ? Guid.NewGuid().ToString() : null;
    ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> ICommandInfo.Parameters => new();
    bool ICommandInfo.AppendErrorBarrier => true;

}

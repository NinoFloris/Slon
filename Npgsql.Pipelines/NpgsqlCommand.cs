using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol;

namespace Npgsql.Pipelines;

public sealed class NpgsqlCommand: DbCommand, ICommandInfo
{
    static ObjectPool<NpgsqlDataReader> ReaderPool { get; } = new(pool =>
    {
        var returnAction = pool.Return;
        return () => new NpgsqlDataReader(returnAction);
    });

    readonly PgV3Protocol _protocol;
    bool _prepare;

    bool _appendErrorBarrier;
    CommandKind _commandKind;
    RowDescription? _rowDescription;
    readonly ChannelWriter<NpgsqlCommand> _multiplexingChannel;
    TaskCompletionSource<IOCompletionPair>? _multiplexingOperation;
    readonly bool _multiplexingCommand;

    internal NpgsqlCommand(PgV3Protocol protocol)
    {
        GC.SuppressFinalize(this);
        _protocol = protocol;
    }

    internal NpgsqlCommand(ChannelWriter<NpgsqlCommand> multiplexingChannel)
    {
        GC.SuppressFinalize(this);
        _multiplexingChannel = multiplexingChannel;
        _multiplexingCommand = true;
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
    protected override DbConnection? DbConnection { get; set; }
    protected override DbParameterCollection DbParameterCollection { get; }
    protected override DbTransaction? DbTransaction { get; set; }
    public override bool DesignTimeVisible { get; set; }

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

    internal void CompleteMultiplexingOperation(IOCompletionPair completionPair)
    {
        _multiplexingOperation!.SetResult(completionPair);
    }

    ValueTask<NpgsqlDataReader> ExecuteDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        IOCompletionPair completionPair;
        // TODO this should instead be 'bring your own operationsource' that we can then await as this way brings too many dispatches.
        // if (_multiplexingCommand)
        // {
        //     _multiplexingOperation ??= new TaskCompletionSource<IOCompletionPair>();
        //     _multiplexingChannel.TryWrite(this);
        //     completionPair = await _multiplexingOperation.Task;
        //     _multiplexingOperation = new TaskCompletionSource<IOCompletionPair>();
        // }
        // else
            completionPair = CommandWriter.WriteExtendedAsync(_protocol, this, flushHint: true, cancellationToken);

        var reader = ReaderPool.Rent();
        return reader.IntializeAsync(completionPair, behavior, cancellationToken);
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        => await ExecuteDataReaderAsync(behavior, cancellationToken);

    CommandKind ICommandInfo.CommandKind => _commandKind;
    string? ICommandInfo.PreparedStatementName => _prepare ? Guid.NewGuid().ToString() : null;
    ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> ICommandInfo.Parameters => new();
    bool ICommandInfo.AppendErrorBarrier => true;

}

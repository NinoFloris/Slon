using System;
using System.Data;
using System.Data.Common;
using System.Net;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol;
using Npgsql.Pipelines.Protocol.PgV3;

namespace Npgsql.Pipelines;

record PgOptions
{
    public required string Username { get; init; }
    public string? Password { get; init; }
    public string? Database { get; init; }
}

record NpgsqlDataSourceOptions
{
    public required EndPoint EndPoint { get; init; }
    public required string Username { get; init; }
    public string? Password { get; init; }
    public string? Database { get; init; }
    public TimeSpan ConnectionTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public int MinPoolSize { get; init; } = 1;
    public int MaxPoolSize { get; init; } = 10;
    public int PoolSize
    {
        init
        {
            MinPoolSize = value;
            MaxPoolSize = value;
        }
    }

    /// <summary>
    /// CommandTimeout affects the first IO read after writing out a command.
    /// Default is infinite, where behavior purely relies on read and write timeouts of the underlying protocol.
    /// </summary>
    public TimeSpan CommandTimeout { get; init; } = Timeout.InfiniteTimeSpan;

    internal PgOptions ToPgOptions() => new()
    {
        Username = Username,
        Database = Database,
        Password = Password
    };

    internal bool Validate()
    {
        // etc
        return true;
    }
}

public class NpgsqlDataSource: DbDataSource, IConnectionFactory<PgV3Protocol>
{
    readonly NpgsqlDataSourceOptions _options;
    readonly PgOptions _pgOptions;
    readonly PgV3ProtocolOptions _pgV3ProtocolOptions;
    readonly ConnectionSource<PgV3Protocol> _connectionSource;
    readonly Channel<(OperationSource Source, ICommandInfo Command, CommandBehavior Behavior, CancellationToken CancellationToken)> _channel;

    internal NpgsqlDataSource(NpgsqlDataSourceOptions options, PgV3ProtocolOptions pgV3ProtocolOptions)
    {
        options.Validate();
        _options = options;
        _pgOptions = options.ToPgOptions();
        _pgV3ProtocolOptions = pgV3ProtocolOptions;
        _connectionSource = new ConnectionSource<PgV3Protocol>(this, options.MaxPoolSize);
        _channel = Channel.CreateBounded<(OperationSource Source, ICommandInfo Command, CommandBehavior Behavior, CancellationToken CancellationToken)>(new BoundedChannelOptions(4096));
        var _ = Task.Run(() => MultiplexingCommandWriter()).ContinueWith(t => t.Exception, TaskContinuationOptions.OnlyOnFaulted);
    }

    internal TimeSpan DefaultConnectionTimeout => _options.ConnectionTimeout;
    internal TimeSpan DefaultCommandTimeout => _options.CommandTimeout;

    internal IOCompletionPair WriteMultiplexingCommand(ICommandInfo command, CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        var source = PgV3Protocol.CreateUnboundOperationSource(cancellationToken);
        if (_channel.Writer.TryWrite((source, command, behavior, cancellationToken)))
            return new IOCompletionPair(new ValueTask<WriteResult>(new WriteResult(WriteResult.UnknownBytesWritten)), source.Task);

        return new IOCompletionPair(WriteCore(this, source, command, behavior, cancellationToken), source.Task);

        static async ValueTask<WriteResult> WriteCore(NpgsqlDataSource instance, OperationSource source, ICommandInfo command, CommandBehavior behavior, CancellationToken cancellationToken)
        {
            await instance._channel.Writer.WriteAsync((source, command, behavior, cancellationToken), cancellationToken);
            return new WriteResult(WriteResult.UnknownBytesWritten);
        }
    }

    internal IOCompletionPair WriteCommand(OperationSlot slot, ICommandInfo command, CommandBehavior behavior, CancellationToken cancellationToken = default) 
        => WriteCommand(slot, command, behavior, flushHint: true, cancellationToken);

    static IOCompletionPair WriteCommand(OperationSlot slot, ICommandInfo command, CommandBehavior behavior, bool flushHint = true, CancellationToken cancellationToken = default)
    {
        return CommandWriter.WriteExtendedAsync(slot, command, flushHint, cancellationToken);
    }

    internal ValueTask<OperationSlot> OpenAsync(bool exclusiveUse, TimeSpan connectionTimeout, CancellationToken cancellationToken = default)
        => _connectionSource.GetAsync(exclusiveUse, connectionTimeout, cancellationToken);

    internal OperationSlot Open(bool exclusiveUse, TimeSpan connectionTimeout)
        => _connectionSource.Get(exclusiveUse, connectionTimeout);

    PgV3Protocol IConnectionFactory<PgV3Protocol>.Create(TimeSpan timeout)
    {
        throw new NotImplementedException();
    }

    async ValueTask<PgV3Protocol> IConnectionFactory<PgV3Protocol>.CreateAsync(CancellationToken cancellationToken)
    {
        var pipes = await PgStreamConnection.ConnectAsync(_options.EndPoint, cancellationToken);
        return await PgV3Protocol.StartAsync(pipes.Writer, pipes.Reader, _pgOptions, _pgV3ProtocolOptions);
    }

    public override string ConnectionString => throw new NotImplementedException();

    protected override DbConnection CreateDbConnection() => new NpgsqlConnection(this);
    public new NpgsqlConnection CreateConnection() => (NpgsqlConnection)CreateDbConnection();
    public new NpgsqlConnection OpenConnection() => (NpgsqlConnection)base.OpenConnection();
    public new async ValueTask<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = CreateConnection();
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    protected override DbCommand CreateDbCommand(string? commandText = null)
        => new NpgsqlCommand(this, true) { CommandText = commandText };

    public new NpgsqlCommand CreateCommand(string? commandText = null)
        => (NpgsqlCommand)CreateDbCommand(commandText);

    protected override void Dispose(bool disposing)
    {
        _channel.Writer.Complete();
    }

    async Task MultiplexingCommandWriter()
    {
        var reader = _channel.Reader;
        var failedToEnqueue = false;
        var writeThreshold = 1000;
        var options = _options;
        var connectionSource = _connectionSource;
        while (failedToEnqueue || await reader.WaitToReadAsync())
        {
            var bytesWritten = 0L;
            PgV3Protocol? protocol = null;
            (OperationSource Source, ICommandInfo Command, CommandBehavior Behavior, CancellationToken CancellationToken) item = default;
            if (failedToEnqueue || reader.TryRead(out item))
            {
                // Bind
                try
                {
                    await connectionSource.BindAsync(item.Source!, options.ConnectionTimeout, item.CancellationToken);
                    protocol = (PgV3Protocol)item.Source!.Protocol!;
                }
                catch (Exception ex)
                {
                    // We need to complete the slot for any error, if it was already canceled that's allowed.
                    item.Source!.TryComplete(ex);
                }
                if (failedToEnqueue)
                {
                    failedToEnqueue = false;
                    if (!reader.TryRead(out item))
                        protocol = null;
                }
            }
            while (protocol is not null && !failedToEnqueue)
            {
                // Write
                IOCompletionPair pair;
                var fewPending = protocol.Pending <= 2;
                try
                {
                    pair = WriteCommand(item.Source!, item.Command!, item.Behavior, flushHint: fewPending, item.CancellationToken);
                }
                catch (Exception ex)
                {
                    item.Source!.TryComplete(ex);
                    break;
                }

                // Flush (if necessary)
                if (fewPending || !pair.Write.IsCompleted || (bytesWritten += pair.Write.Result.BytesWritten) >= writeThreshold || !reader.TryRead(out item))
                {
                    if (!fewPending)
                    {
                        var _ = Flush(pair.Write, protocol);
                    }
                    protocol = null;
                }
                // Next
                else if (!protocol.TryStartOperation(item.Source, cancellationToken: item.CancellationToken))
                    failedToEnqueue = true;
            }
        }

        async ValueTask Flush(ValueTask<WriteResult> write, PgV3Protocol protocol)
        {
            try
            {
                await write;
                await protocol.FlushAsync();
            }
            catch (Exception ex)
            {
                //TODO
                Console.WriteLine(ex.Message);
            }
        }
    }
}

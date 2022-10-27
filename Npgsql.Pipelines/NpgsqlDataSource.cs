using System;
using System.Data;
using System.Data.Common;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
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
    internal static TimeSpan DefaultCommandTimeout = TimeSpan.FromSeconds(30);

    public required EndPoint EndPoint { get; init; }
    public required string Username { get; init; }
    public string? Password { get; init; }
    public string? Database { get; init; }
    public TimeSpan ConnectionTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan CancellationTimeout { get; init; } = TimeSpan.FromSeconds(10);
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
    public TimeSpan CommandTimeout { get; init; } = DefaultCommandTimeout;

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
    readonly ChannelWriter<MultiplexingCommand> _channelWriter;

    internal NpgsqlDataSource(NpgsqlDataSourceOptions options, PgV3ProtocolOptions pgV3ProtocolOptions)
    {
        options.Validate();
        _options = options;
        EndPointRepresentation = options.EndPoint.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6 ? $"tcp://{options.EndPoint}" : options.EndPoint.ToString()!;
        _pgOptions = options.ToPgOptions();
        _pgV3ProtocolOptions = pgV3ProtocolOptions;
        _connectionSource = new ConnectionSource<PgV3Protocol>(this, options.MaxPoolSize);

        var channel = Channel.CreateUnbounded<MultiplexingCommand>();
        _channelWriter = channel.Writer;
        // Make sure to always start on the threadpool.
        var _ = Task.Factory.StartNew(() => MultiplexingCommandWriter(channel.Reader, _connectionSource, options).ContinueWith(t => t.Exception, TaskContinuationOptions.OnlyOnFaulted), CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler: TaskScheduler.Default);
    }

    internal TimeSpan DefaultConnectionTimeout => _options.ConnectionTimeout;
    internal TimeSpan DefaultCancellationTimeout => _options.CancellationTimeout;
    internal TimeSpan DefaultCommandTimeout => _options.CommandTimeout;
    internal string Database => _options.Database ?? _options.Username;
    internal string EndPointRepresentation { get; }

    // TODO should be populated by Start and returned as a a result, cache only once on the datasource, not per connection.
    internal string ServerVersion => throw new NotImplementedException();

    internal void PerformUserCancellation(PgProtocol protocol, TimeSpan timeout)
    {
        // TODO
        // spin up a connection and write out cancel
    }

    internal ValueTask<CommandContextBatch> WriteMultiplexingCommand(NpgsqlCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        var source = PgV3Protocol.CreateUnboundOperationSource(cancellationToken);
        var values = ((ICommand)command).GetValues();
        // TODO these can probably best be pooled.
        var session = new NpgsqlCommandSession(this, values);
        if (_channelWriter.TryWrite(new MultiplexingCommand(source, values, session, cancellationToken)))
            return new ValueTask<CommandContextBatch>(CommandContextBatch.Create(CommandContext.Create(
                new IOCompletionPair(new ValueTask<WriteResult>(WriteResult.Unknown), source.Task),
                values.ExecutionFlags,
                session
            )));

        return WriteAsync(this, source, values, session, behavior, cancellationToken);

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        static async ValueTask<CommandContextBatch> WriteAsync(NpgsqlDataSource instance, OperationSource source, ICommand.Values values, NpgsqlCommandSession session, CommandBehavior behavior, CancellationToken cancellationToken)
        {
            await instance._channelWriter.WriteAsync(new MultiplexingCommand(source, values, session, cancellationToken), cancellationToken);
            return CommandContextBatch.Create(CommandContext.Create(
                new IOCompletionPair(new ValueTask<WriteResult>(WriteResult.Unknown), source.Task),
                values.ExecutionFlags,
                session
            ));
        }
    }

    internal CommandContext WriteCommand(OperationSlot slot, NpgsqlCommand command, CommandBehavior behavior)
    {
        // TODO SingleThreadSynchronizationContext for sync writes happening async.
        return WriteCommandAsync(slot, command, behavior, CancellationToken.None);
    }

    internal CommandContext WriteCommandAsync(OperationSlot slot, ICommand command, CommandBehavior behavior, CancellationToken cancellationToken = default)
        => CommandWriter.WriteExtendedAsync(slot, command, flushHint: true, cancellationToken);

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
        => new NpgsqlCommand(commandText, this);

    public new NpgsqlCommand CreateCommand(string? commandText = null)
        => (NpgsqlCommand)CreateDbCommand(commandText);

    protected override void Dispose(bool disposing)
    {
        _channelWriter.Complete();
    }

    readonly record struct MultiplexingCommand(OperationSource Source, ICommand.Values Values, NpgsqlCommandSession Session, CancellationToken CancellationToken): ICommand
    {
        public ICommand.Values GetValues() => Values;
        public ICommandSession StartSession(in ICommand.Values parameters) => Session;
    }

    static async Task MultiplexingCommandWriter(ChannelReader<MultiplexingCommand> reader, ConnectionSource<PgV3Protocol> connectionSource, NpgsqlDataSourceOptions options)
    {
        var failedToEnqueue = false;
        var writeThreshold = MessageWriter.DefaultAdvisoryFlushThreshold;
        while (failedToEnqueue || await reader.WaitToReadAsync())
        {
            var bytesWritten = 0L;
            PgV3Protocol? protocol = null;
            MultiplexingCommand command = default;
            if (failedToEnqueue || reader.TryRead(out command))
            {
                // Bind slot.
                try
                {
                    await connectionSource.BindAsync(command.Source, options.ConnectionTimeout, command.CancellationToken);
                    protocol = (PgV3Protocol)command.Source.Protocol!;
                }
                catch (Exception ex)
                {
                    // We need to complete the slot for any error, if it was already canceled that's allowed.
                    command.Source.TryComplete(ex);
                }
                if (failedToEnqueue)
                {
                    failedToEnqueue = false;
                    if (!reader.TryRead(out command))
                        protocol = null;
                }
            }
            while (protocol is not null && !failedToEnqueue)
            {
                // Write command.
                ValueTask<WriteResult> writeTask;
                var fewPending = protocol.Pending <= 2;
                try
                {
                    writeTask = CommandWriter.WriteExtendedAsync(command.Source, command, flushHint: fewPending, command.CancellationToken).WriteTask;
                }
                catch (Exception ex)
                {
                    command.Source.TryComplete(ex);
                    break;
                }

                // Flush (if necessary).
                var didFlush = fewPending;
                if (!didFlush && (!writeTask.IsCompleted || (bytesWritten += writeTask.Result.BytesWritten) >= writeThreshold || !reader.TryRead(out command)))
                {
                    var _ = Flush(writeTask, protocol);
                    protocol = null;
                }
                else if (didFlush)
                    protocol = null;
                // Next.
                else if (!protocol.TryStartOperation(command.Source, cancellationToken: command.CancellationToken))
                    failedToEnqueue = true;
            }
        }

        async ValueTask Flush(ValueTask<WriteResult> writeTask, PgV3Protocol protocol)
        {
            try
            {
                await writeTask;
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

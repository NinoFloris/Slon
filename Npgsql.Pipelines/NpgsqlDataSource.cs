using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

public class NpgsqlDataSource: DbDataSource, IConnectionFactory<PgV3Protocol>, ICommandExecutionProvider
{
    readonly NpgsqlDataSourceOptions _options;
    readonly PgOptions _pgOptions;
    readonly PgV3ProtocolOptions _pgV3ProtocolOptions;
    readonly ConnectionSource<PgV3Protocol> _connectionSource;
    readonly ChannelWriter<OperationSource> _channelWriter;

    internal NpgsqlDataSource(NpgsqlDataSourceOptions options, PgV3ProtocolOptions pgV3ProtocolOptions)
    {
        options.Validate();
        _options = options;
        EndPointRepresentation = options.EndPoint.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6 ? $"tcp://{options.EndPoint}" : options.EndPoint.ToString()!;
        _pgOptions = options.ToPgOptions();
        _pgV3ProtocolOptions = pgV3ProtocolOptions;
        _connectionSource = new ConnectionSource<PgV3Protocol>(this, options.MaxPoolSize);

        var channel = Channel.CreateUnbounded<OperationSource>(new UnboundedChannelOptions()
        {
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        _channelWriter = channel.Writer;
        // Make sure to always start on the threadpool.
        var _ = RunOnThreadPool(() => MultiplexingCommandWriter(this, channel.Reader, _connectionSource, options).ContinueWith(t => t.Exception, TaskContinuationOptions.OnlyOnFaulted));
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

    readonly ConcurrentDictionary<int, CommandExecution> _trackedCommandExecutions = new();
    // Can wrap around, is ok.
    int _executionId;

    readonly struct MultiplexingItem: ICommand
    {
        public MultiplexingItem(int executionId, ICommand.Values values)
        {
            ExecutionId = executionId;
            Values = values;
        }

        public int ExecutionId { get; }
        public ICommand.Values Values { get; }
        public ICommand.Values GetValues() => Values;
        public ICommandSession StartSession(in ICommand.Values parameters) => throw new NotImplementedException();

        public void Deconstruct(out int executionId, out ICommand.Values values)
        {
            executionId = ExecutionId;
            values = Values;
        }
    }

    internal ValueTask<CommandContextBatch> WriteMultiplexingCommand(ICommand command, CancellationToken cancellationToken = default)
    {
        var item = new MultiplexingItem(Interlocked.Increment(ref _executionId), command.GetValues());
        var source = PgV3Protocol.CreateUnboundOperationSource(item, cancellationToken);

        if (_channelWriter.TryWrite(source))
            return new ValueTask<CommandContextBatch>(CommandContextBatch.Create(CommandContext.Create(
                new IOCompletionPair(new ValueTask<WriteResult>(WriteResult.Unknown), source.Task),
                item.ExecutionId,
                this
            )));

        return WriteAsync(this, item.ExecutionId, source, cancellationToken);

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        static async ValueTask<CommandContextBatch> WriteAsync(NpgsqlDataSource instance, int executionId, OperationSource source, CancellationToken cancellationToken)
        {
            await instance._channelWriter.WriteAsync(source, cancellationToken).ConfigureAwait(false);
            return CommandContextBatch.Create(CommandContext.Create(
                new IOCompletionPair(new ValueTask<WriteResult>(WriteResult.Unknown), source.Task),
                executionId,
                instance
            ));
        }
    }

    internal CommandContext WriteCommand(OperationSlot slot, ICommand command)
    {
        // TODO SingleThreadSynchronizationContext for sync writes happening async.
        return WriteCommandAsync(slot, command, CancellationToken.None);
    }

    internal CommandContext WriteCommandAsync(OperationSlot slot, ICommand command, CancellationToken cancellationToken = default)
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

    static async Task MultiplexingCommandWriter(NpgsqlDataSource instance, ChannelReader<OperationSource> reader, ConnectionSource<PgV3Protocol> connectionSource, NpgsqlDataSourceOptions options)
    {
        const int writeThreshold = 1000; //MessageWriter.DefaultAdvisoryFlushThreshold;
        var failedToEnqueue = false;
        while (failedToEnqueue || await reader.WaitToReadAsync())
        {
            var bytesWritten = 0L;
            PgV3Protocol? protocol = null;
            OperationSource source = null!;
            try
            {
                if (failedToEnqueue || reader.TryRead(out source!))
                {
                    // Bind slot, this might throw.
                    await connectionSource.BindAsync(source, options.ConnectionTimeout, source.CancellationToken).ConfigureAwait(false);
                    protocol = (PgV3Protocol)source.Protocol!;

                    if (failedToEnqueue)
                    {
                        failedToEnqueue = false;
                        if (!reader.TryRead(out source!))
                            protocol = null;
                    }
                }

                while (protocol is not null && !failedToEnqueue)
                {
                    var fewPending = protocol.Pending <= 2;

                    // We need to prefill the session slot, before writing to prevent any races, as the read slot could already be completed.
                    var (executionId, values) = PgV3Protocol.GetData<MultiplexingItem>(source);
                    var flags = CommandWriter.GetEffectiveExecutionFlags(source, values, out var statementName);
                    // We allocate only to prepare statements.
                    var session = flags.HasPreparing() ? new NpgsqlCommandSession(instance, values) : null;

                    instance._trackedCommandExecutions[executionId] = flags switch
                    {
                        _ when flags.HasPrepared() => CommandExecution.Create(flags, statement: null!), // TODO lookup statement
                        _ when session is null => CommandExecution.Create(flags),
                        _ => CommandExecution.Create(flags, session)
                    };

                    // Write command, might throw.
                    var writeTask = CommandWriter.WriteExtendedAsync(source, values, session, statementName, flushHint: fewPending, source.CancellationToken).WriteTask;

                    // Flush (if necessary).
                    var didFlush = fewPending;
                    // TODO we may want to keep track of protocols that are flushing so even if it has the least pending we don't pick it.
                    if (!didFlush && (!writeTask.IsCompleted || (bytesWritten += writeTask.Result.BytesWritten) >= writeThreshold || !reader.TryRead(out source!)))
                    {
                        // We don't need to await writeTask because flushasync will wait on the lock to release, which the writetask would be holding until completion.
                        // All FlushAsync code is inside an async method, any exceptions will be stored on the task.
                        var task = protocol.FlushAsync();
                        if (!task.IsCompletedSuccessfully)
                        {
                            var _ = task.AsTask().ContinueWith(t =>
                            {
                                try
                                {
                                    t.GetAwaiter().GetResult();
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine(ex.Message);
                                }
                            }, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.DenyChildAttach);
                        }

                        protocol = null;
                    }
                    else if (didFlush)
                        protocol = null;
                    // Next.
                    else if (!protocol.TryStartOperation(source, cancellationToken: source.CancellationToken))
                        failedToEnqueue = true;
                }
            }
            catch (Exception openOrWriteException)
            {
                try
                {
                    // Connection is borked.
                    source?.TryComplete(openOrWriteException);
                }
                catch(Exception completionException)
                {
                    Console.WriteLine(completionException.Message);
                }
            }
        }
    }

    static Task<TResult> RunOnThreadPool<TResult>(Func<TResult> func)
        => Task.Factory.StartNew(func, CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler: TaskScheduler.Default);

    static Task<TResult> RunOnThreadPool<TResult>(Func<object?, TResult> func, object? state)
        => Task.Factory.StartNew(func, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler: TaskScheduler.Default);

    CommandExecution ICommandExecutionProvider.Get(in CommandContext context)
    {
        var result = context.TryGetSessionId(out var executionId);
        DebugShim.Assert(result);
        // Let it throw if it's not in there.
        var commandExecution = _trackedCommandExecutions[executionId];
        _trackedCommandExecutions.TryRemove(executionId, out _);
        // _trackedCommandExecutions.TryRemove(new KeyValuePair<int, CommandExecution>(executionId, commandExecution));
        return commandExecution;
    }
}

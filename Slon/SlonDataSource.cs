using System;
using System.Collections.Immutable;
using System.Data.Common;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Slon.Data;
using Slon.Pg;
using Slon.Pg.Types;
using Slon.Protocol;
using Slon.Protocol.Pg;
using Slon.Protocol.PgV3;
using Slon.Protocol.PgV3.Descriptors;

namespace Slon;

interface IFrontendTypeCatalog
{
    DataTypeName GetDataTypeName(PgTypeId pgTypeId);
    bool TryGetIdentifiers(SlonDbType slonDbType, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName);
}

public record SlonDataSourceOptions
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
    public int AutoPrepareMinimumUses { get; set; }

    internal PgOptions ToPgOptions() => new()
    {
        EndPoint = EndPoint,
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

// Multiplexing
public partial class SlonDataSource : ICommandExecutionProvider<CommandExecution>
{
    Channel<OperationSource> CreateChannel() =>
        Channel.CreateUnbounded<OperationSource>(new UnboundedChannelOptions()
        {
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

    struct MultiplexingItem: IPgCommand
    {
        readonly IPgCommand.BeginExecutionDelegate _beginExecutionDelegate;
        IPgCommand.Values _values;
        CommandExecution _commandExecution;

        public MultiplexingItem(IPgCommand.BeginExecutionDelegate beginExecutionDelegate, in IPgCommand.Values values)
        {
            _beginExecutionDelegate = beginExecutionDelegate;
            _values = values;
            _commandExecution = default;
        }

        public CommandExecution CommandExecution
        {
            get => _commandExecution;
            set
            {
                // Null out the values so any heap objects can be freed before the entire operation is done.
                _values = default;
                _commandExecution = value;
            }
        }

        public IPgCommand.Values GetValues() => _values;
        public IPgCommand.BeginExecutionDelegate BeginExecutionMethod => throw new NotSupportedException();
        public CommandExecution BeginExecution(in IPgCommand.Values values) => _beginExecutionDelegate(values);
    }

    static async Task MultiplexingCommandWriter(ChannelReader<OperationSource> reader, SlonDataSource dataSource)
    {
        const int writeThreshold = 1000; // TODO arbitrary constant, it works well though...
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
                    await dataSource.ConnectionSource.BindAsync(source, dataSource.ConnectionTimeout, source.CancellationToken).ConfigureAwait(false);
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
                    // TODO this should probably only trigger if it also wrote a substantial amount (as a crude proxy for query compute cost)
                    var fewPending = false; // pending <= 2;

                    // D
                    // Write command, might throw.
                    var writeTask = WriteCommand(dataSource.GetDbDependencies().CommandWriter, source, flushHint: fewPending);

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
                    // TODO
                    Console.WriteLine(completionException.Message);
                }
            }
        }

        static ValueTask<WriteResult> WriteCommand(PgV3CommandWriter commandWriter, OperationSource source, bool flushHint)
        {
            ref var command = ref PgV3Protocol.GetDataRef<MultiplexingItem>(source);
            var commandContext = commandWriter.WriteAsync(source, command, flushHint, source.CancellationToken);
            command.CommandExecution = commandContext.GetCommandExecution();
            return commandContext.WriteTask;
        }
    }

    CommandExecution ICommandExecutionProvider<CommandExecution>.Get(in CommandContext<CommandExecution> context)
        => PgV3Protocol.GetDataRef<MultiplexingItem>(context.ReadSlot).CommandExecution;

#if !NETSTANDARD2_0
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
    internal async ValueTask<CommandContextBatch<CommandExecution>> WriteMultiplexingCommand<TCommand>(TCommand command, CancellationToken cancellationToken = default)
        where TCommand: IPgCommand
    {
        await EnsureInitializedAsync(cancellationToken);

        var item = new MultiplexingItem(command.BeginExecutionMethod, command.GetValues());
        var source = PgV3Protocol.CreateUnboundOperationSource(item, cancellationToken);
        await ChannelWriter.WriteAsync(source, cancellationToken).ConfigureAwait(false);
        return CommandContext<CommandExecution>.Create(new IOCompletionPair(new (WriteResult.Unknown), source), this);
    }
}

class PgDatabaseInfo
{
    public PgDatabaseInfo(PgTypeCatalog typeCatalog)
    {
        TypeCatalog = typeCatalog;
        ServerVersion = "PG";
    }

    public string ServerVersion { get; }

    public PgTypeCatalog TypeCatalog { get; }
}


interface IPgDatabaseInfoProvider
{
    PgDatabaseInfo Get(PgOptions pgOptions, TimeSpan timeSpan);
    ValueTask<PgDatabaseInfo> GetAsync(PgOptions pgOptions, CancellationToken cancellationToken = default);
}

class DefaultPgDatabaseInfoProvider: IPgDatabaseInfoProvider
{
    PgDatabaseInfo Create() => new(PgTypeCatalog.Default);
    public PgDatabaseInfo Get(PgOptions pgOptions, TimeSpan timeSpan) => Create();
    public ValueTask<PgDatabaseInfo> GetAsync(PgOptions pgOptions, CancellationToken cancellationToken = default) => new(Create());
}
public partial class SlonDataSource: DbDataSource, IConnectionFactory<PgV3Protocol>
{
    readonly SlonDataSourceOptions _options;
    readonly PgOptions _pgOptions;
    readonly PgV3ProtocolOptions _pgV3ProtocolOptions;
    readonly IPgDatabaseInfoProvider _pgDatabaseInfoProvider;
    readonly IdentityFacetsTransformer _facetsTransformer;
    readonly SemaphoreSlim _lifecycleLock;

    // Initialized on the first real use.
    ConnectionSource<PgV3Protocol>? _connectionSource;
    ChannelWriter<OperationSource>? _channelWriter;
    PgDbDependencies? _dbDependencies;
    bool _isInitialized;

    public SlonDataSource(string connectionString)
        : this(new()
        {
            EndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 5432),
            Username = "postgres",
            Password = "postgres123",
        }, new(), null)
    {

    }

    internal SlonDataSource(SlonDataSourceOptions options, PgV3ProtocolOptions pgV3ProtocolOptions, IPgDatabaseInfoProvider? pgDatabaseInfoProvider = null)
    {
        options.Validate();
        _options = options;
        EndPointRepresentation = options.EndPoint.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6 ? $"tcp://{options.EndPoint}" : options.EndPoint.ToString()!;
        _pgOptions = options.ToPgOptions();
        _pgV3ProtocolOptions = pgV3ProtocolOptions;
        _pgDatabaseInfoProvider = pgDatabaseInfoProvider ?? new DefaultPgDatabaseInfoProvider();
        _lifecycleLock = new(1);
        _facetsTransformer = new IdentityFacetsTransformer();
    }

    Exception NotInitializedException() => new InvalidOperationException("DataSource is not initialized yet, at least one connection needs to be opened first.");

    ChannelWriter<OperationSource> ChannelWriter => _channelWriter ?? throw NotInitializedException();
    ConnectionSource<PgV3Protocol> ConnectionSource => _connectionSource ?? throw NotInitializedException();

    // Store the result if multiple dependencies are required. The instance may be switched out during reloading.
    // To prevent any inconsistencies without having to obtain a lock on the data we instead use an immutable instance.
    // All relevant depedencies are bundled to provide a consistent view, it's either all new or all old data.
    PgDbDependencies GetDbDependencies() => _dbDependencies ?? throw NotInitializedException();

    // False for datasources that dispatch commands across different backends.
    // Among other effects this impacts cacheability of state derived from unstable backend type information.
    // Its value should be static for the lifetime of the instance.
    internal bool IsPhysicalDataSource => true;
    // This is to get back to the multi-host datasource that owns its host sources.
    // It also helps commands to keep caches intact when switching sources from the same owner.
    internal SlonDataSource DataSourceOwner => this;

    internal TimeSpan ConnectionTimeout => _options.ConnectionTimeout;
    internal TimeSpan DefaultCancellationTimeout => _options.CancellationTimeout;
    internal TimeSpan DefaultCommandTimeout => _options.CommandTimeout;
    internal string Database => _options.Database ?? _options.Username;
    internal string EndPointRepresentation { get; }

    internal string ServerVersion => GetDbDependencies().PgDatabaseInfo.ServerVersion;

    int DbDepsRevision { get; set; }

     ValueTask Initialize(bool async, CancellationToken cancellationToken)
    {
        if (_isInitialized)
            return new ValueTask();

        return Core();

        async ValueTask Core()
        {
            if (async)
                await _lifecycleLock.WaitAsync(cancellationToken);
            else
                _lifecycleLock.Wait(cancellationToken);
            try
            {
                if (_isInitialized)
                    return;

                // We don't flow cancellationToken past this point, at least one thread has to finish the init.
                // We do DbDeps first as it may throw, otherwise we'd need to cleanup the other dependencies again.
                _dbDependencies = await CreateDbDeps(async, Timeout.InfiniteTimeSpan, CancellationToken.None); // TODO for now we could hook up the right things (init timeout?) later.

                var channel = CreateChannel();
                _channelWriter = channel.Writer;
                _connectionSource = new ConnectionSource<PgV3Protocol>(this, _options.MaxPoolSize);
                var _ = Task.Run(() => MultiplexingCommandWriter(channel.Reader, this).ContinueWith(t => t.Exception, TaskContinuationOptions.OnlyOnFaulted));
                _isInitialized = true;
                // We insert a memory barrier to make sure _isInitialized is published to all processors before we release the semaphore.
                // This is needed to be sure no other initialization will be started on another core that doesn't see _isInitialized = true yet but was already waiting for the lock.
                Thread.MemoryBarrier();
            }
            finally
            {
                _lifecycleLock.Release();
            }
        }
    }

    void EnsureInitialized() => Initialize(false, CancellationToken.None).GetAwaiter().GetResult();
    ValueTask EnsureInitializedAsync(CancellationToken cancellationToken) => Initialize(true, cancellationToken);

    async ValueTask<PgDbDependencies> CreateDbDeps(bool async, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var databaseInfo = async
            ? _pgDatabaseInfoProvider.Get(_pgOptions, timeout)
            : await _pgDatabaseInfoProvider.GetAsync(_pgOptions, cancellationToken);

        var converterOptions = new PgConverterOptions
        {
            TypeCatalog = databaseInfo.TypeCatalog,
            TextEncoding = _pgOptions.Encoding,
            ConverterInfoResolver = new DefaultConverterInfoResolver()
        };

        return new PgDbDependencies(databaseInfo, converterOptions, new StatementTracker(_options.AutoPrepareMinimumUses), DbDepsRevision++);
    }

    internal void PerformUserCancellation(Protocol.Protocol protocol, TimeSpan timeout)
    {
        // TODO spin up a connection and write out cancel
    }

    internal CommandContext<CommandExecution> WriteCommand<TCommand>(OperationSlot slot, TCommand command) where TCommand: IPgCommand
    {
        EnsureInitialized();
        // TODO SingleThreadSynchronizationContext for sync writes happening async.
        return GetDbDependencies().CommandWriter.WriteAsync(slot, command, flushHint: true, CancellationToken.None);
    }

    internal async ValueTask<CommandContext<CommandExecution>> WriteCommandAsync<TCommand>(OperationSlot slot, TCommand command, CancellationToken cancellationToken = default)
        where TCommand : IPgCommand
    {
        await EnsureInitializedAsync(cancellationToken);
        return GetDbDependencies().CommandWriter.WriteAsync(slot, command, flushHint: true, cancellationToken);
    }

    internal async ValueTask<OperationSlot> GetSlotAsync(bool exclusiveUse, TimeSpan connectionTimeout, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        return await ConnectionSource.GetAsync(exclusiveUse, connectionTimeout, cancellationToken);
    }

    internal OperationSlot GetSlot(bool exclusiveUse, TimeSpan connectionTimeout)
    {
        EnsureInitialized();
        return ConnectionSource.Get(exclusiveUse, connectionTimeout);
    }

    PgV3Protocol IConnectionFactory<PgV3Protocol>.Create(TimeSpan timeout)
    {
        throw new NotImplementedException();
    }

    async ValueTask<PgV3Protocol> IConnectionFactory<PgV3Protocol>.CreateAsync(CancellationToken cancellationToken)
    {
        var pipes = await PgStreamConnection.ConnectAsync(_options.EndPoint, cancellationToken);
        return await PgV3Protocol.StartAsync(pipes.Writer, pipes.Reader, _pgOptions, _pgV3ProtocolOptions);
    }

    internal string SensitiveConnectionString => throw new NotImplementedException();
    public override string ConnectionString => ""; //TODO

    protected override DbConnection CreateDbConnection() => new SlonConnection(this);
    public new SlonConnection CreateConnection() => (SlonConnection)CreateDbConnection();
    public new SlonConnection OpenConnection() => (SlonConnection)base.OpenConnection();
    public new async ValueTask<SlonConnection> OpenConnectionAsync(CancellationToken cancellationToken)
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
        => new SlonCommand(commandText, this);

    public new SlonCommand CreateCommand(string? commandText = null)
        => (SlonCommand)CreateDbCommand(commandText);

    protected override void Dispose(bool disposing)
    {
        _channelWriter?.Complete();
    }

    internal PgConverterInfo<T>? GetConverterInfo<T>()
    {
        var dbDeps = GetDbDependencies();
        return (PgConverterInfo<T>?)dbDeps.ConverterOptions.GetConverterInfo(typeof(T));
    }

    internal ParameterContextFactory GetParameterContextFactory(string? statementText = null)
    {
        var dbDeps = GetDbDependencies();
        return new(dbDeps, _facetsTransformer, dbDeps.ParameterContextBuilderFactory,
            PgV3CommandWriter.EstimateParameterBufferSize(PgStreamConnection.WriterSegmentSize, statementText));
    }

    internal Statement CreateCommandStatement(PgTypeIdView parameterTypes)
    {
        if (parameterTypes.IsEmpty)
            return PgV3Statement.CreateUnprepared(PreparationKind.Command);

        // We can return backend specific statements when this is a physical data source.
        if (IsPhysicalDataSource)
        {
            var dbDeps = GetDbDependencies();
            var typeNamesBuilder = ImmutableArray.CreateBuilder<PgTypeId>(parameterTypes.Length);
            var statementParameterBuilder = ImmutableArray.CreateBuilder<StatementParameter>(parameterTypes.Length);
            foreach (var name in parameterTypes)
            {
                typeNamesBuilder.Add(name);
                statementParameterBuilder.Add(new(dbDeps.TypeCatalog.GetOid(name)));
            }

            return PgV3Statement.CreateUnprepared(PreparationKind.Command, typeNamesBuilder.MoveToImmutable(), statementParameterBuilder.MoveToImmutable());
        }
        else
        {
            var typeNamesBuilder = ImmutableArray.CreateBuilder<PgTypeId>(parameterTypes.Length);
            foreach (var name in parameterTypes)
                typeNamesBuilder.Add(name);

            return PgV3Statement.CreateDbAgnostic(PreparationKind.Command, typeNamesBuilder.MoveToImmutable());
        }
    }

    internal Statement? GetStatement(string statementText, PgTypeIdView parameterTypes)
    {
        var dbDeps = GetDbDependencies();
        if (dbDeps.StatementsTracker.LookupForUse(statementText, parameterTypes) is { } statement)
            return statement;

        if (parameterTypes.IsEmpty)
            statement = dbDeps.StatementsTracker.Add(PgV3Statement.CreateUnprepared(PreparationKind.Auto));
        else
        {
            var typeNamesBuilder = ImmutableArray.CreateBuilder<PgTypeId>(parameterTypes.Length);
            var statementParameterBuilder = ImmutableArray.CreateBuilder<StatementParameter>(parameterTypes.Length);
            foreach (var name in parameterTypes)
            {
                typeNamesBuilder.Add(name);
                statementParameterBuilder.Add(new(dbDeps.TypeCatalog.GetOid(name)));
            }

            var result = PgV3Statement.CreateUnprepared(PreparationKind.Auto, typeNamesBuilder.MoveToImmutable(), statementParameterBuilder.MoveToImmutable());
            statement = dbDeps.StatementsTracker.Add(result);
        }

        return statement;
    }

    // Internal for testing.
    internal class PgDbDependencies: IFrontendTypeCatalog
    {
        public PgDbDependencies(PgDatabaseInfo pgDatabaseInfo, PgConverterOptions converterOptions, StatementTracker statementsTracker, int revision)
        {
            PgDatabaseInfo = pgDatabaseInfo;
            CommandWriter = new(pgDatabaseInfo.TypeCatalog, converterOptions.TextEncoding, MapAgnosticStatement);
            ConverterOptions = converterOptions;
            StatementsTracker = statementsTracker;
            Revision = revision;
            ParameterContextBuilderFactory = GetParameterContextBuilder;
        }

        public PgDatabaseInfo PgDatabaseInfo { get; }
        public PgV3CommandWriter CommandWriter { get; }
        public PgConverterOptions ConverterOptions { get; }
        public StatementTracker StatementsTracker { get; }
        public int Revision { get; }

        public ParameterContextBuilderFactory ParameterContextBuilderFactory { get; }
        public PgTypeCatalog TypeCatalog => PgDatabaseInfo.TypeCatalog;

        ParameterContextBuilder GetParameterContextBuilder(int length, int estimatedParameterBufferLength)
            => new(length, estimatedParameterBufferLength, Revision, ConverterOptions);

        PgV3Statement MapAgnosticStatement(PgV3Statement statement)
        {
            // We don't track statements for individual commands at the data source level.
            if (statement.Kind is PreparationKind.Command)
                return Map(statement, PgDatabaseInfo.TypeCatalog);

            if (StatementsTracker.Lookup(statement.Id) is { } mapped)
                return mapped;

            mapped = Map(statement, PgDatabaseInfo.TypeCatalog);
            StatementsTracker.Add(mapped);
            return mapped;

            static PgV3Statement Map(PgV3Statement statement, PgTypeCatalog pgTypecatalog)
            {
                if (statement.ParameterTypes.IsEmpty)
                    return PgV3Statement.CreateUnprepared(PreparationKind.Command);

                var parameterTypeNames = statement.ParameterTypes;
                var typeNamesBuilder = ImmutableArray.CreateBuilder<PgTypeId>(parameterTypeNames.Length);
                var statementParameterBuilder = ImmutableArray.CreateBuilder<StatementParameter>(parameterTypeNames.Length);
                foreach (var name in parameterTypeNames)
                {
                    typeNamesBuilder.Add(name);
                    statementParameterBuilder.Add(new(pgTypecatalog.GetOid(name)));
                }

                return PgV3Statement.CreateUnprepared(PreparationKind.Command, typeNamesBuilder.MoveToImmutable(), statementParameterBuilder.MoveToImmutable());
            }
        }

        public bool TryGetIdentifiers(SlonDbType slonDbType, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
            => TryGetIdentifiers(TypeCatalog, slonDbType, out canonicalTypeId, out dataTypeName);

        public DataTypeName GetDataTypeName(PgTypeId typeId) => TypeCatalog.GetDataTypeName(typeId);

        internal static bool TryGetIdentifiers(PgTypeCatalog typeCatalog, SlonDbType slonDbType, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
        {
            if (slonDbType.ResolveArrayType)
                return typeCatalog.TryGetArrayIdentifiers(slonDbType.DataTypeName, out canonicalTypeId, out dataTypeName);

            if (slonDbType.ResolveMultiRangeType)
                return typeCatalog.TryGetMultiRangeIdentifiers(slonDbType.DataTypeName, out canonicalTypeId, out dataTypeName);

            return typeCatalog.TryGetIdentifiers(slonDbType.DataTypeName, out canonicalTypeId, out dataTypeName);
        }
    }
}

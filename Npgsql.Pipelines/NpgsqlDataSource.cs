using System;
using System.Data;
using System.Data.Common;
using System.Net;
using System.Threading;
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

public class NpgsqlDataSource: DbDataSource, IConnectionFactory<PgV3Protocol>
{
    readonly EndPoint _endPoint;
    readonly PgOptions _pgOptions;
    readonly PgV3ProtocolOptions _pgV3ProtocolOptions;
    readonly ConnectionSource<PgV3Protocol> _connectionSource;

    internal NpgsqlDataSource(EndPoint endPoint, PgOptions pgOptions, PgV3ProtocolOptions pgV3ProtocolOptions)
    {
        _endPoint = endPoint;
        _pgOptions = pgOptions;
        _pgV3ProtocolOptions = pgV3ProtocolOptions;
        _connectionSource = new ConnectionSource<PgV3Protocol>(this, 10);
    }

    internal IOCompletionPair WriteMultiplexingCommand(NpgsqlCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        var slot = PgV3Protocol.CreateUnboundOperationSlot();
        // Do the little channel dance.
        throw new NotImplementedException();
        // return new IOCompletionPair(new ValueTask(), slot.Task);
    }

    internal IOCompletionPair WriteCommand(OperationSlot slot, NpgsqlCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        return CommandWriter.WriteExtendedAsync(slot, command, flushHint: true, cancellationToken);
    }

    internal ValueTask<OperationSlot> OpenAsync(bool exclusiveUse, TimeSpan connectionTimeout = default, CancellationToken cancellationToken = default) => _connectionSource.GetAsync(exclusiveUse, cancellationToken);
    internal OperationSlot Open(bool exclusiveUse, TimeSpan connectionTimeout = default) => throw new NotImplementedException();

    PgV3Protocol IConnectionFactory<PgV3Protocol>.Create(TimeSpan timeout)
    {
        throw new NotImplementedException();
    }

    async ValueTask<PgV3Protocol> IConnectionFactory<PgV3Protocol>.CreateAsync(CancellationToken cancellationToken)
    {
        var pipes = await PgStreamConnection.ConnectAsync(_endPoint, cancellationToken);
        return await PgV3Protocol.StartAsync(pipes.Writer, pipes.Reader, _pgOptions, _pgV3ProtocolOptions);
    }

    protected override DbConnection CreateDbConnection()
    {
        throw new NotImplementedException();
    }

    protected override DbCommand CreateDbCommand(string? commandText = null)
    {
        return new NpgsqlCommand(this, true) { CommandText = commandText };
    }

    public override string ConnectionString => throw new NotImplementedException();
}

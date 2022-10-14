using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol;

namespace Npgsql.Pipelines;

enum ReaderState
{
    BeforeResult,
    InResult,
    BetweenResults,
    Consumed,
    Closed,
    Disposed,
}

public sealed class NpgsqlDataReader: DbDataReader
{
    readonly PgV3Protocol _protocol;
    readonly CommandReader _commandReader;
    ReadActivation _readActivation;
    ReaderState _state;

    internal NpgsqlDataReader(PgV3Protocol protocol)
    {
        _protocol = protocol;
        _commandReader = new CommandReader(protocol);
    }

    internal async ValueTask IntializeAsync(ReadActivation readActivation)
    {
        _readActivation = readActivation;
        // Immediately initialize the first result set, we're supposed to be positioned there at the start.
        await _commandReader.InitializeAsync().ConfigureAwait(false);
    }

    public override int Depth => 0;
    public override int FieldCount => _commandReader.FieldCount;

    public override object this[int ordinal] => throw new NotImplementedException();

    public override object this[string name] => throw new NotImplementedException();

    public override int RecordsAffected { get; }
    public override bool HasRows { get; }
    public override bool IsClosed => _state > ReaderState.Closed || _state == ReaderState.Disposed;

    
    public override void Close()
    {
        throw new NotImplementedException();
    }


    // TODO Could check completed here or in nextresultasync after awaiting and make sure Rfq is buffered, so we can do all that async, dispose would be guaranteed not to do IO.
    public override Task<bool> ReadAsync(CancellationToken cancellationToken) =>
        _commandReader.ReadAsync(cancellationToken);

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public override bool GetBoolean(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override byte GetByte(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override DateTime GetDateTime(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override decimal GetDecimal(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override double GetDouble(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override Type GetFieldType(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override float GetFloat(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override Guid GetGuid(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override short GetInt16(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetInt32(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetInt64(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override string GetName(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetOrdinal(string name)
    {
        throw new NotImplementedException();
    }

    public override string GetString(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override object GetValue(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetValues(object[] values)
    {
        throw new NotImplementedException();
    }

    public override bool IsDBNull(int ordinal)
    {
        throw new NotImplementedException();
    }
    
    public override bool NextResult()
    {
        throw new NotImplementedException();
    }

    public override bool Read()
    {
        throw new NotImplementedException();
    }


    public override IEnumerator GetEnumerator()
    {
        throw new NotImplementedException();
    }

    protected override void Dispose(bool disposing)
    {
        _readActivation?.Complete();
        // TODO Make sure _commandReader is completed (consume any remaining commands and rfq etc).
    }

#if !NETSTANDARD
    public override Task CloseAsync()
    {
        throw new NotImplementedException();
    }

    public override ValueTask DisposeAsync()
    {
        _readActivation?.Complete();
        // TODO Make sure _commandReader is completed (consume any remaining commands and rfq etc).
        return new ValueTask();
    }
#endif
}

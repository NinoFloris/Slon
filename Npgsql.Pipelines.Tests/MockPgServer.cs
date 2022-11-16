using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;
using Npgsql.Pipelines.Pg.Descriptors;
using Npgsql.Pipelines.Pg.Types;
using Npgsql.Pipelines.Protocol.PgV3;
using Npgsql.Pipelines.Protocol.PgV3.Descriptors;
using NUnit.Framework;
using FlushResult = System.IO.Pipelines.FlushResult;

namespace Npgsql.Pipelines.Tests;

class MockPgServer : IDisposable
{
    static readonly Encoding Encoding = Encoding.UTF8;

    bool _disposed;

    const int BackendSecret = 12345;
    internal int ProcessId { get; }

    readonly IDuplexPipe _serverPipe;
    StreamingWriter<PipeStreamingWriter> _writer;
    ref StreamingWriter<PipeStreamingWriter> Writer => ref _writer;
    readonly SimplePipeReader _pipeReader;

    public MockPgServer(int processId)
    {
        ProcessId = processId;

        var input = new Pipe();
        var output = new Pipe();

        var serverOutput = output.Writer;
        var clientInput = input.Reader;
        _serverPipe = new DuplexPipe(clientInput, serverOutput);

        Writer = new StreamingWriter<PipeStreamingWriter>(new PipeStreamingWriter(_serverPipe.Output));
        _pipeReader = new SimplePipeReader(_serverPipe.Input, TimeSpan.Zero);

        var serverInput = output.Reader;
        var clientOutput = input.Writer;
        ClientPipe = new DuplexPipe(serverInput, clientOutput);
    }

    public IDuplexPipe ClientPipe { get; }

    public StatementField CreateStatementField(PgType type)
        => new(new Field("?", type), -1, 0, (Oid)0, 0, FormatCode.Binary);

    public async Task Startup(MockState state)
    {
        // Read and skip the startup message
        await SkipStartupMessage();

        WriteAuthenticateOk();
        var parameters = new Dictionary<string, string>
        {
            { "server_version", "14" },
            { "server_encoding", "UTF8" },
            { "client_encoding", "UTF8" },
            { "application_name", "Mock" },
            { "is_superuser", "on" },
            { "session_authorization", "foo" },
            { "DateStyle", "ISO, MDY" },
            { "IntervalStyle", "postgres" },
            { "TimeZone", "UTC" },
            { "integer_datetimes", "on" },
            { "standard_conforming_strings", "on" }
        };
        // While PostgreSQL 14 always sends default_transaction_read_only and in_hot_standby, we only send them if requested
        // To minimize potential issues for tests not requiring multiple hosts
        if (state != MockState.MultipleHostsDisabled)
        {
            parameters["default_transaction_read_only"] = state == MockState.Primary ? "off" : "on";
            parameters["in_hot_standby"] = state == MockState.Standby ? "on" : "off";
        }
        WriteParameterStatuses(parameters);
        WriteBackendKeyData(ProcessId, BackendSecret);
        WriteReadyForQuery();
        await FlushAsync();
    }

    public async Task FailedStartup(string errorCode)
    {
        // Read and skip the startup message
        await SkipStartupMessage();
        WriteErrorResponse(errorCode);
        await FlushAsync();
    }

    public async Task SkipStartupMessage()
    {
        var buffer = await _pipeReader.ReadAtLeastAsync(sizeof(int));
        var length = ReadPayloadLength(buffer);
        _pipeReader.Advance(sizeof(int));
        var _ = await _pipeReader.ReadAtLeastAsync(length);
        _pipeReader.Advance(length);
    }

    public Task SendMockState(MockState state)
    {
        var isStandby = state == MockState.Standby;
        var transactionReadOnly = state is MockState.Standby or MockState.PrimaryReadOnly
            ? "on"
            : "off";

        return WriteParseComplete()
            .WriteBindComplete()
            .WriteRowDescription(CreateStatementField(WellKnownTypes.Bool))
            .WriteDataRow(BitConverter.GetBytes(isStandby))
            .WriteCommandComplete()
            .WriteParseComplete()
            .WriteBindComplete()
            .WriteRowDescription(CreateStatementField(WellKnownTypes.Text))
            .WriteDataRow(Encoding.ASCII.GetBytes(transactionReadOnly))
            .WriteCommandComplete()
            .WriteReadyForQuery()
            .FlushAsync();
    }

    static int ReadPayloadLength(ReadOnlySequence<byte> buffer)
    {
        var reader = new SequenceReader<byte>(buffer);
        reader.TryReadBigEndian(out int length);
        return length - 4;
    }

    static FrontendCode ReadCode(ReadOnlySequence<byte> buffer)
    {
        var reader = new SequenceReader<byte>(buffer);
        reader.TryRead(out byte code);
        return (FrontendCode)code;
    }

    public async Task IsExpected(FrontendCode expectedCode)
    {
        ThrowIfDisposed();

        var buffer = await _pipeReader.ReadAtLeastAsync(sizeof(int) + 1);
        var code = ReadCode(buffer);

        Assert.AreEqual(expectedCode, code, $"Expected code: {expectedCode}, instead got: {code}");

        var length = ReadPayloadLength(buffer.Slice(1));
        _pipeReader.Advance(sizeof(int) + 1);
        await _pipeReader.ReadAtLeastAsync(length);
        _pipeReader.Advance(length);
    }

    public Task ConsumeExtendedQuery()
        => AreExpected(
            FrontendCode.Parse,
            FrontendCode.Bind,
            FrontendCode.Describe,
            FrontendCode.Execute,
            FrontendCode.Sync);

    public async Task AreExpected(params FrontendCode[] expectedCodes)
    {
        foreach (var expectedCode in expectedCodes)
            await IsExpected(expectedCode);
    }

    public async Task FlushAsync()
    {
        ThrowIfDisposed();
        await _serverPipe.Output.FlushAsync();
    }

    public void Complete()
    {
        _serverPipe.Input.Complete();
        _serverPipe.Output.Complete();
    }

    public MockPgServer WriteParseComplete()
    {
        ThrowIfDisposed();
        Writer.WriteByte((byte)BackendCode.ParseComplete);
        Writer.WriteInt(4);
        return this;
    }

    public MockPgServer WriteBindComplete()
    {
        ThrowIfDisposed();
        Writer.WriteByte((byte)BackendCode.BindComplete);
        Writer.WriteInt(4);
        return this;
    }

    public MockPgServer WriteRowDescription(params StatementField[] fields)
    {
        ThrowIfDisposed();

        Writer.WriteByte((byte)BackendCode.RowDescription);
        Writer.WriteInt(4 + 2 + fields.Sum(f => Encoding.GetByteCount(f.Field.Name) + 1 + 18));
        Writer.WriteShort((short)fields.Length);

        foreach (var field in fields)
        {
            Writer.WriteCString(field.Field.Name);
            Writer.WriteUInt(field.TableOid);
            Writer.WriteShort(field.ColumnAttributeNumber);
            Writer.WriteUInt(field.Field.Oid);
            Writer.WriteShort(field.FieldTypeSize);
            Writer.WriteInt(field.FieldTypeModifier);
            Writer.WriteShort((short)field.FormatCode);
        }

        return this;
    }

    public MockPgServer WriteNoData()
    {
        ThrowIfDisposed();
        Writer.WriteByte((byte)BackendCode.NoData);
        Writer.WriteInt(4);
        return this;
    }

    public MockPgServer WriteDataRow(params byte[][] columnValues)
    {
        ThrowIfDisposed();

        Writer.WriteByte((byte)BackendCode.DataRow);
        Writer.WriteInt(4 + 2 + columnValues.Sum(v => 4 + v.Length));
        Writer.WriteShort((short)columnValues.Length);

        foreach (var field in columnValues)
        {
            Writer.WriteInt(field.Length);
            Writer.WriteRaw(field);
        }

        return this;
    }

    public MockPgServer WriteCommandComplete(string tag = "")
    {
        ThrowIfDisposed();

        Writer.WriteByte((byte)BackendCode.CommandComplete);
        Writer.WriteInt(4 + Encoding.GetByteCount(tag) + 1);
        Writer.WriteCString(tag);
        return this;
    }

    public MockPgServer WriteReadyForQuery(TransactionStatus transactionStatus = TransactionStatus.Idle)
    {
        ThrowIfDisposed();
        Writer.WriteByte((byte)BackendCode.ReadyForQuery);
        Writer.WriteInt(4 + 1);
        Writer.WriteByte((byte)transactionStatus);
        return this;
    }

    public MockPgServer WriteAuthenticateOk()
    {
        ThrowIfDisposed();
        Writer.WriteByte((byte)BackendCode.AuthenticationRequest);
        Writer.WriteInt(4 + 4);
        Writer.WriteInt(0);
        return this;
    }

    public MockPgServer WriteParameterStatuses(Dictionary<string, string> parameters)
    {
        foreach (var kv in parameters)
            WriteParameterStatus(kv.Key, kv.Value);
        return this;
    }

    public MockPgServer WriteParameterStatus(string name, string value)
    {
        ThrowIfDisposed();

        Writer.WriteByte((byte)BackendCode.ParameterStatus);
        Writer.WriteInt(4 + Encoding.GetByteCount(name) + 1 + Encoding.GetByteCount(value) + 1);
        Writer.WriteCString(name);
        Writer.WriteCString(value);

        return this;
    }

    public MockPgServer WriteBackendKeyData(int processId, int secret)
    {
        ThrowIfDisposed();
        Writer.WriteByte((byte)BackendCode.BackendKeyData);
        Writer.WriteInt(4 + 4 + 4);
        Writer.WriteInt(processId);
        Writer.WriteInt(secret);
        return this;
    }

    public MockPgServer WriteCancellationResponse()
        => WriteErrorResponse("57014", "Cancellation", "Query cancelled"); // 57014 is query canceled.

    public MockPgServer WriteErrorResponse(string code)
        => WriteErrorResponse(code, "ERROR", "MOCK ERROR MESSAGE");

    public MockPgServer WriteErrorResponse(string code, string severity, string message)
    {
        ThrowIfDisposed();
        Writer.WriteByte((byte)BackendCode.ErrorResponse);
        Writer.WriteInt(
            4 +
            1 + Encoding.GetByteCount(code) +
            1 + Encoding.GetByteCount(severity) +
            1 + Encoding.GetByteCount(message) +
            1);
        Writer.WriteByte((byte)ErrorResponse.ErrorFieldTypeCode.Code);
        Writer.WriteCString(code);
        Writer.WriteByte((byte)ErrorResponse.ErrorFieldTypeCode.Severity);
        Writer.WriteCString(severity);
        Writer.WriteByte((byte)ErrorResponse.ErrorFieldTypeCode.Message);
        Writer.WriteCString(message);
        Writer.WriteByte((byte)ErrorResponse.ErrorFieldTypeCode.Done);
        Writer.Commit();
        return this;
    }

    void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(MockPgServer));
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
    }
}

static class MockPgServerExtensions
{
    public static Task WriteScalarResponseAndFlush(this MockPgServer server, int value)
        => server.WriteParseComplete()
            .WriteBindComplete()
            .WriteRowDescription(server.CreateStatementField(WellKnownTypes.Int4))
            .WriteDataRow(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(value)))
            .WriteCommandComplete()
            .WriteReadyForQuery()
            .FlushAsync();

    public static Task WriteScalarResponseAndFlush(this MockPgServer server, bool value)
        => server.WriteParseComplete()
            .WriteBindComplete()
            .WriteRowDescription(server.CreateStatementField(WellKnownTypes.Bool))
            .WriteDataRow(BitConverter.GetBytes(value))
            .WriteCommandComplete()
            .WriteReadyForQuery()
            .FlushAsync();

    public static Task WriteScalarResponseAndFlush(this MockPgServer server, string value)
        => server.WriteParseComplete()
            .WriteBindComplete()
            .WriteRowDescription(server.CreateStatementField(WellKnownTypes.Text))
            .WriteDataRow(Encoding.UTF8.GetBytes(value))
            .WriteCommandComplete()
            .WriteReadyForQuery()
            .FlushAsync();
}

enum MockState
{
    MultipleHostsDisabled = 0,
    Primary = 1,
    PrimaryReadOnly = 2,
    Standby = 3
}

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;
using Npgsql.Pipelines.MiscMessages;
using Npgsql.Pipelines.QueryMessages;
using Npgsql.Pipelines.StartupMessages;

namespace Npgsql.Pipelines;

record ConnectionOptions
{
    public required string Username { get; init; }
    public string? Password { get; init; }
    public string? Database { get; init; }
}

class PgV3Protocol : IDisposable
{
    readonly SimplePipeReader _reader;
    FlushableBufferWriter<PipeWriter> _writer;

    readonly ArrayPool<FieldDescription> _fieldDescriptionPool = ArrayPool<FieldDescription>.Create(RowDescription.MaxColumns, 50);

    // Lock held for a message write, writes to the pipe for one message shouldn't be interleaved with another.
    readonly SemaphoreSlim _messageWriteLock = new(1);
    readonly HeaderBufferWriter _headerBufferWriter = new();

    PgV3Protocol(PipeWriter writer, PipeReader reader)
    {
        _writer = FlushableBufferWriter.Create(writer);
        _reader = new SimplePipeReader(reader);
    }

    public async ValueTask WriteMessageAsync<T>(T message) where T : IFrontendMessage
    {
        await _messageWriteLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (message is IStreamingFrontendMessage streamingMessage)
            {
                try
                {
                    var result = await streamingMessage.WriteWithHeaderAsync(new MessageWriter<FlushableBufferWriter<PipeWriter>>(_writer)).ConfigureAwait(false);
                    // Completed then do something.
                    // if (result.IsCompleted)
                }
                catch (Exception)
                {
                    throw;
                    // _log.LogError(0, ex, $"Unexpected exception in {nameof(TimingPipeFlusher)}.{nameof(FlushAsync)}.");
                }
                return;
            }

            int length = 0;
            try
            {
                if (message.TryPrecomputeLength(out length))
                {
                    length += MessageWriter.IntByteCount;
                    var writer = new MessageWriter<FlushableBufferWriter<PipeWriter>>(_writer);
                    writer.WriteByte((byte)message.FrontendCode);
                    writer.WriteInt(length);
                    writer.Commit();
                    message.Write(writer);
                }
                else
                {
                    message.Write(new MessageWriter<HeaderBufferWriter>(_headerBufferWriter));
                    // Completed then do something.
                    _headerBufferWriter.SetCode((byte)message.FrontendCode);
                    _headerBufferWriter.CopyTo(_writer);
                }
            }
            catch (Exception)
            {
                throw;
                // _log.LogError(0, ex, $"Unexpected exception in {nameof(TimingPipeFlusher)}.{nameof(FlushAsync)}.");
            }
            finally
            {
                if (length == 0)
                    _headerBufferWriter.Reset();
            }
        }
        finally
        {
            var result = await _writer.FlushAsync();
            _messageWriteLock.Release();
        }
    }

    ValueTask<ReadOnlySequence<byte>> ReadAsync(long ensureLength = MessageHeader.CodeAndLengthByteCount, CancellationToken cancellationToken = default)
    {
        if (!_reader.TryRead(ensureLength, out var buf))
            return MoveNextAsyncCore(_reader, ensureLength, cancellationToken);

        return new(buf);

        static async ValueTask<ReadOnlySequence<byte>> MoveNextAsyncCore(SimplePipeReader reader, long ensureLength, CancellationToken cancellationToken)
        {
            await reader.WaitForDataAsync(ensureLength, cancellationToken);
            var res = reader.TryRead(ensureLength, out var buffer);
            Debug.Assert(res, "TryRead should always be successful after an async wait for data.");
            return buffer;
        }
    }

    public async ValueTask<T> ReadMessageAsync<T>(T message, CancellationToken cancellationToken = default)
        where T : IBackendMessage
    {
        ReadStatus status;
        MessageReader.ResumptionData? resumptionData = null;
        long consumed = 0;
        ReadOnlySequence<byte> sequence = default;
        Exception? readerExn = null;
        do
        {
            if (consumed == 0)
                sequence = await ReadAsync(resumptionData is null ?
                    MessageHeader.CodeAndLengthByteCount :
                    Math.Min(MessageHeader.CodeAndLengthByteCount, resumptionData.Value.Header.Length - resumptionData.Value.MessageIndex),
                    cancellationToken).ConfigureAwait(false);
            try
            {
                status = ReadCore(ref message, sequence, ref resumptionData, ref consumed);
            }
            catch(Exception ex)
            {
                // Readers aren't supposed to throw, when we have logging do that here.
                status = ReadStatus.InvalidData;
                readerExn = ex;
            }

            switch (status)
            {
                case ReadStatus.Done:
                case ReadStatus.NeedMoreData:
                    _reader.Advance(consumed);
                    consumed = 0;
                    break;
                case ReadStatus.InvalidData:
                    // Try to read error response.
                    Exception exn;
                    if (readerExn is null && resumptionData?.Header.IsDefault == false && resumptionData.Value.Header.Code == BackendCode.ErrorResponse)
                    {
                        var errorResponse = new ErrorResponse();
                        consumed -= resumptionData.Value.MessageIndex;
                        // Let it start clean, as if it has to MoveNext for the first time.
                        resumptionData = null;
                        var errorResponseStatus = ReadCore(ref errorResponse, sequence, ref resumptionData, ref consumed);
                        if (errorResponseStatus != ReadStatus.Done)
                            exn = new Exception($"Unexpected error on message: {typeof(T).FullName}, could not read full error response, terminated connection.");
                        else
                            exn = new Exception($"Unexpected error on message: {typeof(T).FullName}, error message: {errorResponse.ErrorOrNoticeMessage.Message}.");
                    }
                    else
                    {
                        exn = new Exception($"Protocol desync on message: {typeof(T).FullName}, expected different response.", readerExn);
                    }
                    _reader.Complete(exn);
                    throw exn;
                case ReadStatus.AsyncResponse:
                    ReadStatus asyncResponseStatus;
                    do
                    {
                        asyncResponseStatus = HandleAsyncResponse(sequence, ref resumptionData, ref consumed);
                        switch (asyncResponseStatus)
                        {
                            case ReadStatus.AsyncResponse:
                                throw new Exception("Should never happen, async response handling should not return ReadStatus.AsyncResponse.");
                            case ReadStatus.InvalidData:
                                throw new Exception("Should never happen, any unknown data during async response handling should be left for the original message handler.");
                            case ReadStatus.NeedMoreData:
                                _reader.Advance(consumed);
                                consumed = 0;
                                sequence = await ReadAsync(resumptionData is null ?
                                    MessageHeader.CodeAndLengthByteCount :
                                    Math.Min(MessageHeader.CodeAndLengthByteCount, resumptionData.Value.Header.Length - resumptionData.Value.MessageIndex),cancellationToken).ConfigureAwait(false);
                                break;
                            case ReadStatus.Done:
                                // We don't reset consumed here, the original handler may continue where we left.
                                break;
                        }
                    } while (asyncResponseStatus != ReadStatus.Done);
                    break;
            }
        } while (status != ReadStatus.Done);

        return message;

        // As MessageReader is a ref struct we need a small method to create it and pass a reference.
        static ReadStatus ReadCore<TMessage>(ref TMessage message, in ReadOnlySequence<byte> sequence, ref MessageReader.ResumptionData? resumptionData, ref long consumed) where TMessage: IBackendMessage
        {
            var reader = resumptionData is null ? MessageReader.Create(sequence) :
                consumed == 0 ? MessageReader.Resume(sequence, resumptionData.Value) : MessageReader.Create(sequence, resumptionData.Value, consumed);
            if (resumptionData is null && consumed != 0)
                reader.Reader.Advance(consumed);
            var status = message.Read(ref reader);
            consumed = reader.Consumed;
            if (status != ReadStatus.Done)
                resumptionData = reader.GetResumptionData();

            return status;
        }
    }

    public ValueTask<T> ReadMessageAsync<T>(CancellationToken cancellationToken = default) where T : IBackendMessage, new()
        => ReadMessageAsync(new T(), cancellationToken);

    ReadStatus HandleAsyncResponse(in ReadOnlySequence<byte> buffer, ref MessageReader.ResumptionData? resumptionData, ref long consumed)
    {
        var reader = consumed == 0 ? MessageReader.Resume(buffer, resumptionData!.Value) : MessageReader.Create(buffer, resumptionData!.Value, consumed);

        consumed = reader.Consumed;
        throw new NotImplementedException();
    }

    public static async ValueTask<PgV3Protocol> StartAsync(PipeWriter writer, PipeReader reader, ConnectionOptions options)
    {
        try
        {
            var conn = new PgV3Protocol(writer, reader);
            await conn.WriteMessageAsync(new StartupRequest(options)).ConfigureAwait(false);
            var msg = await conn.ReadMessageAsync(new AuthenticationResponse()).ConfigureAwait(false);
            switch (msg.AuthenticationType)
            {
                case AuthenticationType.Ok:
                    await conn.ReadMessageAsync<StartupResponse>().ConfigureAwait(false);
                    break;
                case AuthenticationType.MD5Password:
                    if (options.Password is null)
                        throw new InvalidOperationException("No password given, connection expects password.");
                    await conn.WriteMessageAsync(new PasswordMessage(options.Username, options.Password, msg.MD5Salt)).ConfigureAwait(false);
                    var expectOk = await conn.ReadMessageAsync(new AuthenticationResponse()).ConfigureAwait(false);
                    if (expectOk.AuthenticationType != AuthenticationType.Ok)
                        throw new Exception("Unexpected authentication response");
                    await conn.ReadMessageAsync<StartupResponse>().ConfigureAwait(false);
                    break;
                case AuthenticationType.CleartextPassword:
                default:
                    throw new Exception();
            }

            var portal = string.Empty;
            await conn.WriteMessageAsync(new Parse("SELECT * from generate_series(1, 10)", new ArraySegment<CommandParameter>()));
            await conn.WriteMessageAsync(new Bind(portal, new ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>(), ResultColumnCodes.CreateOverall(FormatCode.Binary)));
            await conn.WriteMessageAsync(new Describe(DescribeName.CreateForPortal(portal)));
            await conn.WriteMessageAsync(new Execute(portal));
            await conn.WriteMessageAsync(new Sync());
            await conn.ReadMessageAsync<ParseComplete>();
            await conn.ReadMessageAsync<BindComplete>();
            var description = await conn.ReadMessageAsync(new RowDescription(conn._fieldDescriptionPool));

            var dataReader = new DataReader(conn, description);
            while (await dataReader.ReadAsync())
            {
                var i = await dataReader.GetFieldValueAsync<int>();
                i = i;
            }

            return conn;
        }
        catch (Exception)
        {
            writer.Complete();
            reader.Complete();
            throw;
        }
    }

    public void Dispose()
    {
        _reader.Complete();
        _writer.Writer.Complete();
        _messageWriteLock.Dispose();
    }
}


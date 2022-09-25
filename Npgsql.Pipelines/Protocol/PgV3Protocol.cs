using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;
using Npgsql.Pipelines.MiscMessages;
using Npgsql.Pipelines.QueryMessages;
using Npgsql.Pipelines.StartupMessages;

namespace Npgsql.Pipelines;

record ConnectionOptions
{
    public int ReaderSegmentSize { get; init; } = 8096;
    public int WriterSegmentSize { get; init; } = 8096;
}

record PgOptions
{
    public required string Username { get; init; }
    public string? Password { get; init; }
    public string? Database { get; init; }
}

class PgV3Protocol : IDisposable
{
    readonly ConnectionOptions _connectionOptions;
    readonly SimplePipeReader _reader;
    readonly FlushableBufferWriter<PipeWriter> _writer;
    readonly FlushableBufferWriter<IPipeWriterSyncSupport> _writerSync;
    readonly ArrayPool<FieldDescription> _fieldDescriptionPool = ArrayPool<FieldDescription>.Create(RowDescription.MaxColumns, 50);

    // Lock held for a message write, writes to the pipe for one message shouldn't be interleaved with another.
    readonly SemaphoreSlim _messageWriteLock = new(1);
    HeaderBufferWriter? _headerBufferWriter;

    PgV3Protocol(IPipeWriterSyncSupport writer, IPipeReaderSyncSupport reader, ConnectionOptions connectionOptions)
    {
        _connectionOptions = connectionOptions;
        _writer = FlushableBufferWriter.Create(writer.PipeWriter);
        _writerSync = FlushableBufferWriter.Create(writer);
        _reader = new SimplePipeReader(reader);
    }

    PgV3Protocol(PipeWriter writer, PipeReader reader, ConnectionOptions connectionOptions)
    {
        _connectionOptions = connectionOptions;
        _writer = FlushableBufferWriter.Create(writer);
        _writerSync = null!;
        _reader = new SimplePipeReader(new AsyncOnlyPipeReader(reader));
    }

    static bool IsAsync(CancellationTokenOrTimeout cancellationToken) => cancellationToken.IsCancellationToken;

    public ValueTask WriteMessageAsync<T>(T message, CancellationToken cancellationToken = default) where T : IFrontendMessage
        => WriteMessageCore(message, CancellationTokenOrTimeout.CreateCancellationToken(cancellationToken));

    public void WriteMessage<T>(T message, TimeSpan timeout = default) where T : IFrontendMessage
        => WriteMessageCore(message, CancellationTokenOrTimeout.CreateTimeout(timeout)).GetAwaiter().GetResult();

    async ValueTask WriteMessageCore<T>(T message, CancellationTokenOrTimeout cancelationToken) where T : IFrontendMessage
    {
        var isAsync = IsAsync(cancelationToken);
        if (isAsync && !_messageWriteLock.Wait(0))
            await _messageWriteLock.WaitAsync().ConfigureAwait(false);
        else if (!isAsync)
            _messageWriteLock.Wait(cancelationToken.Timeout);
        try
        {
            if (message is IStreamingFrontendMessage streamingMessage)
            {
                var result = isAsync ? await streamingMessage.WriteWithHeaderAsync(new MessageWriter<FlushableBufferWriter<PipeWriter>>(_writer)).ConfigureAwait(false) :
                                         streamingMessage.WriteWithHeaderAsync(new MessageWriter<FlushableBufferWriter<IPipeWriterSyncSupport>>(_writerSync)).GetAwaiter().GetResult();
                // Complete it again but now with any updates to our protocol state.
                if (result.IsCompleted)
                    await CompletePipeWriter(_writer.Writer, isAsync);
            }
            else if (message.TryPrecomputeLength(out var precomputedLength))
            {
                if (precomputedLength < 0)
                    throw new InvalidOperationException("TryPrecomputeLength out value \"length\" cannot be negative.");

                precomputedLength += MessageWriter.IntByteCount;
                var writer = new MessageWriter<FlushableBufferWriter<PipeWriter>>(_writer);
                writer.WriteByte((byte)message.FrontendCode);
                writer.WriteInt(precomputedLength);
                writer.Commit();
                message.Write(writer);
            }
            else
            {
                try
                {
                    _headerBufferWriter ??= new HeaderBufferWriter();
                    message.Write(new MessageWriter<HeaderBufferWriter>(_headerBufferWriter));
                    // Completed then do something.
                    _headerBufferWriter.SetCode((byte)message.FrontendCode);
                    _headerBufferWriter.CopyTo(_writer);
                }
                finally
                {
                    _headerBufferWriter!.Reset();
                }
            }
        }
        catch (Exception ex)
        {
            await CompletePipeWriter(_writer.Writer, isAsync, ex);
            throw;
        }
        finally
        {
            var result = isAsync ? await _writer.FlushAsync(cancelationToken) : _writerSync.FlushAsync(cancelationToken).GetAwaiter().GetResult();
            // Complete it again but now with any updates to our protocol state.
            if (result.IsCompleted)
                await CompletePipeWriter(_writer.Writer, isAsync);
            _messageWriteLock.Release();
        }

        static async ValueTask CompletePipeWriter(PipeWriter pipeWriter, bool isAsync, Exception? exception = null)
        {
            if (isAsync)
                await pipeWriter.CompleteAsync(exception);
            else
                pipeWriter.Complete(exception);
        }
    }

    public ValueTask<T> ReadMessageAsync<T>(T message, CancellationToken cancellationToken = default) where T : IBackendMessage =>
        ReadMessageCore(message, CancellationTokenOrTimeout.CreateCancellationToken(cancellationToken));

    public ValueTask<T> ReadMessageAsync<T>(CancellationToken cancellationToken = default) where T : IBackendMessage, new()
        => ReadMessageCore(new T(), CancellationTokenOrTimeout.CreateCancellationToken(cancellationToken));

    public T ReadMessage<T>(T message, TimeSpan timeout = default) where T : IBackendMessage =>
        ReadMessageCore(message, CancellationTokenOrTimeout.CreateTimeout(timeout)).GetAwaiter().GetResult();

    public T ReadMessage<T>(TimeSpan timeout = default) where T : IBackendMessage, new() =>
        ReadMessageCore(new T(), CancellationTokenOrTimeout.CreateTimeout(timeout)).GetAwaiter().GetResult();

    async ValueTask<T> ReadMessageCore<T>(T message, CancellationTokenOrTimeout cancellationToken = default)
        where T : IBackendMessage
    {
        ReadStatus status;
        MessageReader.ResumptionData resumptionData = default;
        long consumed = 0;
        Exception? readerExn = null;
        var isAsync = IsAsync(cancellationToken);
        do
        {
            var buffer = isAsync
                ? await ReadAsync(ComputeMinimumSize(resumptionData), cancellationToken.CancellationToken).ConfigureAwait(false)
                : Read(ComputeMinimumSize(resumptionData), cancellationToken.Timeout);

            try
            {
                status = ReadCore(ref message, buffer, ref resumptionData, ref consumed);
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
                    var exception = CreateUnexpectedError(buffer, resumptionData, consumed, readerExn);
                    await CompletePipeReader(isAsync, exception);
                    throw exception;
                case ReadStatus.AsyncResponse:
                    ReadStatus asyncResponseStatus;
                    do
                    {
                        asyncResponseStatus = HandleAsyncResponse(buffer, ref resumptionData, ref consumed);
                        switch (asyncResponseStatus)
                        {
                            case ReadStatus.AsyncResponse:
                                throw new Exception("Should never happen, async response handling should not return ReadStatus.AsyncResponse.");
                            case ReadStatus.InvalidData:
                                throw new Exception("Should never happen, any unknown data during async response handling should be left for the original message handler.");
                            case ReadStatus.NeedMoreData:
                                _reader.Advance(consumed);
                                consumed = 0;
                                buffer = isAsync
                                    ? await ReadAsync(ComputeMinimumSize(resumptionData), cancellationToken.CancellationToken).ConfigureAwait(false)
                                    : Read(ComputeMinimumSize(resumptionData), cancellationToken.Timeout);
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
        static ReadStatus ReadCore<TMessage>(ref TMessage message, in ReadOnlySequence<byte> sequence, ref MessageReader.ResumptionData resumptionData, ref long consumed) where TMessage: IBackendMessage
        {
            MessageReader reader;
            if (resumptionData.IsDefault)
            {
                reader = MessageReader.Create(sequence);
                if (consumed != 0)
                    reader.Reader.Advance(consumed);
            }
            else if (consumed == 0)
                reader = MessageReader.Resume(sequence, resumptionData);
            else
                reader = MessageReader.Create(sequence, resumptionData, consumed);

            var status = message.Read(ref reader);
            consumed = reader.Consumed;
            if (status != ReadStatus.Done)
                resumptionData = reader.GetResumptionData();

            return status;
        }

        async ValueTask CompletePipeReader(bool isAsync, Exception? exception = null)
        {
            if (isAsync)
                await _reader.CompleteAsync(exception);
            else
                _reader.Complete(exception);
        }

        int ComputeMinimumSize(in MessageReader.ResumptionData resumptionData)
        {
            if (resumptionData.IsDefault)
                // TODO does this assumption always hold?
                return MessageHeader.CodeAndLengthByteCount;

            var remainingMessage = (int)(resumptionData.Header.Length - resumptionData.MessageIndex);

            // TODO move this into a throw helper.
            if (remainingMessage == 0)
                throw new InvalidOperationException("Message reader asked for more data yet we're on a message that is fully consumed.");

            if (remainingMessage < MessageHeader.CodeAndLengthByteCount)
                return remainingMessage;

            // Don't ask for the full message given the reader may want to stream it, just ask for more data.
            return remainingMessage < _connectionOptions.ReaderSegmentSize ? remainingMessage : _connectionOptions.ReaderSegmentSize;
        }

        [DoesNotReturn]
        static Exception CreateUnexpectedError(ReadOnlySequence<byte> buffer, scoped in MessageReader.ResumptionData resumptionData, long consumed, Exception? readerException = null)
        {
            // Try to read error response.
            Exception exception;
            if (readerException is null && resumptionData.IsDefault == false && resumptionData.Header.Code == BackendCode.ErrorResponse)
            {
                var errorResponse = new ErrorResponse();
                consumed -= resumptionData.MessageIndex;
                // Let it start clean, as if it has to MoveNext for the first time.
                MessageReader.ResumptionData emptyResumptionData = default;
                var errorResponseStatus = ReadCore(ref errorResponse, buffer, ref emptyResumptionData, ref consumed);
                if (errorResponseStatus != ReadStatus.Done)
                    exception = new Exception($"Unexpected error on message: {typeof(T).FullName}, could not read full error response, terminated connection.");
                else
                    exception = new Exception($"Unexpected error on message: {typeof(T).FullName}, error message: {errorResponse.ErrorOrNoticeMessage.Message}.");
            }
            else
            {
                exception = new Exception($"Protocol desync on message: {typeof(T).FullName}, expected different response.", readerException);
            }
            return exception;
        }

        static ReadStatus HandleAsyncResponse(in ReadOnlySequence<byte> buffer, scoped ref MessageReader.ResumptionData resumptionData, ref long consumed)
        {
            var reader = consumed == 0 ? MessageReader.Resume(buffer, resumptionData) : MessageReader.Create(buffer, resumptionData, consumed);

            consumed = reader.Consumed;
            throw new NotImplementedException();
        }

        ValueTask<ReadOnlySequence<byte>> ReadAsync(int minimumSize, CancellationToken cancellationToken)
        {
            if (!_reader.TryRead(minimumSize, out var buffer))
                return _reader.ReadAtLeastAsync(minimumSize, cancellationToken);

            return new(buffer);
        }

        ReadOnlySequence<byte> Read(int minimumSize, TimeSpan timeout)
        {
            if (!_reader.TryRead(minimumSize, out var buffer))
                return _reader.ReadAtLeast(minimumSize, timeout);

            return buffer;
        }
    }

    static async ValueTask<PgV3Protocol> StartAsyncCore(PgV3Protocol conn, PgOptions options)
    {
        try
        {
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
            await conn.WriteMessageAsync(new Parse("SELECT ' from generate_series(1, 10)", new ArraySegment<CommandParameter>())).ConfigureAwait(false);
            await conn.WriteMessageAsync(new Bind(portal, new ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>(), ResultColumnCodes.CreateOverall(FormatCode.Binary))).ConfigureAwait(false);
            await conn.WriteMessageAsync(new Describe(DescribeName.CreateForPortal(portal))).ConfigureAwait(false);
            await conn.WriteMessageAsync(new Execute(portal)).ConfigureAwait(false);
            await conn.WriteMessageAsync(new Sync()).ConfigureAwait(false);
            await conn.ReadMessageAsync<ParseComplete>().ConfigureAwait(false);
            await conn.ReadMessageAsync<BindComplete>().ConfigureAwait(false);
            var description = await conn.ReadMessageAsync(new RowDescription(conn._fieldDescriptionPool)).ConfigureAwait(false);

            var dataReader = new DataReader(conn, description);
            while (await dataReader.ReadAsync().ConfigureAwait(false))
            {
                var i = await dataReader.GetFieldValueAsync<int>().ConfigureAwait(false);;
                i = i;
            }

            return conn;
        }
        catch (Exception ex)
        {
            conn.Dispose();
            throw;
        }
    }

    public static ValueTask<PgV3Protocol> StartAsync(PipeWriter writer, PipeReader reader, PgOptions options, ConnectionOptions? pipeOptions = null)
    {
        var conn = new PgV3Protocol(writer, reader, pipeOptions ?? new ConnectionOptions());
        return StartAsyncCore(conn, options);
    }

    public static ValueTask<PgV3Protocol> StartAsync(IPipeWriterSyncSupport writer, IPipeReaderSyncSupport reader, PgOptions options, ConnectionOptions? pipeOptions = null)
    {
        var conn = new PgV3Protocol(writer, reader, pipeOptions ?? new ConnectionOptions());
        return StartAsyncCore(conn, options);
    }

    public static PgV3Protocol Start(IPipeWriterSyncSupport writer, IPipeReaderSyncSupport reader, PgOptions options, ConnectionOptions? pipeOptions = null)
    {
        try
        {
            var conn = new PgV3Protocol(writer, reader, pipeOptions ?? new ConnectionOptions());
            conn.WriteMessage(new StartupRequest(options));
            var msg = conn.ReadMessage(new AuthenticationResponse());
            switch (msg.AuthenticationType)
            {
                case AuthenticationType.Ok:
                    conn.ReadMessage<StartupResponse>();
                    break;
                case AuthenticationType.MD5Password:
                    if (options.Password is null)
                        throw new InvalidOperationException("No password given, connection expects password.");
                    conn.WriteMessage(new PasswordMessage(options.Username, options.Password, msg.MD5Salt));
                    var expectOk = conn.ReadMessage(new AuthenticationResponse());
                    if (expectOk.AuthenticationType != AuthenticationType.Ok)
                        throw new Exception("Unexpected authentication response");
                    conn.ReadMessage<StartupResponse>();
                    break;
                case AuthenticationType.CleartextPassword:
                default:
                    throw new Exception();
            }

            var portal = string.Empty;
            conn.WriteMessage(new Parse("SELECT ' from generate_series(1, 10)", new ArraySegment<CommandParameter>()));
            conn.WriteMessage(new Bind(portal, new ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>>(), ResultColumnCodes.CreateOverall(FormatCode.Binary)));
            conn.WriteMessage(new Describe(DescribeName.CreateForPortal(portal)));
            conn.WriteMessage(new Execute(portal));
            conn.WriteMessage(new Sync());
            conn.ReadMessage<ParseComplete>();
            conn.ReadMessage<BindComplete>();
            var description = conn.ReadMessage(new RowDescription(conn._fieldDescriptionPool));

            // var dataReader = new DataReader(conn, description);
            // while (await dataReader.ReadAsync())
            // {
            //     var i = await dataReader.GetFieldValueAsync<int>();
            //     i = i;
            // }

            return conn;
        }
        catch (Exception ex)
        {
            writer.PipeWriter.Complete(ex);
            reader.PipeReader.Complete(ex);
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



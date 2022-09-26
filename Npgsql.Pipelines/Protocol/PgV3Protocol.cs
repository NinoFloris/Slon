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
using FlushResult = Npgsql.Pipelines.Buffers.FlushResult;

namespace Npgsql.Pipelines;

record ProtocolOptions
{
    public TimeSpan ReadTimeout { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan WriteTimeout { get; init;  } = TimeSpan.FromSeconds(1);
    /// <summary>
    /// CommandTimeout affects the first IO read after writing out a command.
    /// Default is infinite, where behavior purely relies on read and write timeouts.
    /// </summary>
    public TimeSpan CommandTimeout { get; init; } = Timeout.InfiniteTimeSpan;
    public int ReaderSegmentSize { get; init; } = 8096;
    public int WriterSegmentSize { get; init; } = 8096;

    public Func<DbParameter, KeyValuePair<CommandParameter, IParameterWriter>> ParameterWriterLookup { get; init; }
}

record PgOptions
{
    public required string Username { get; init; }
    public string? Password { get; init; }
    public string? Database { get; init; }
}

class PgV3Protocol : IDisposable
{
    static ProtocolOptions DefaultPipeOptions { get; } = new();
    readonly ProtocolOptions _protocolOptions;
    readonly SimplePipeReader _reader;
    readonly PipeWriter _pipeWriter;
    readonly ArrayPool<FieldDescription> _fieldDescriptionPool = ArrayPool<FieldDescription>.Create(RowDescription.MaxColumns, 50);

    // Lock held for a message write, writes to the pipe for one message shouldn't be interleaved with another.
    readonly SemaphoreSlim _messageWriteLock = new(1);
    MessageWriter<HeaderBufferWriter>? _headerBufferWriter;
    readonly ResettableFlushControl _flushControl;
    readonly MessageWriter<IPipeWriterSyncSupport> _defaultMessageWriter;
    bool _writerCompleted;

    PgV3Protocol(IPipeWriterSyncSupport writer, IPipeReaderSyncSupport reader, ProtocolOptions? protocolOptions = null)
    {
        _protocolOptions = protocolOptions ?? DefaultPipeOptions;
        _pipeWriter = writer.PipeWriter;
        _flushControl = new ResettableFlushControl(writer, _protocolOptions.WriteTimeout, _protocolOptions.WriterSegmentSize);
        _defaultMessageWriter = new MessageWriter<IPipeWriterSyncSupport>(writer, _flushControl);
        _reader = new SimplePipeReader(reader);
    }

    PgV3Protocol(PipeWriter writer, PipeReader reader, ProtocolOptions protocolOptions)
        : this(new AsyncOnlyPipeWriter(writer), new AsyncOnlyPipeReader(reader), protocolOptions)
    { }

    public async ValueTask WriteMessageAsync<T>(T message, CancellationToken cancellationToken = default) where T : IFrontendMessage
    {
        if (!_messageWriteLock.Wait(0))
            await _messageWriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        _flushControl.Initialize();
        try
        {
            await ProcessFlushResultAsync(await UnsynchronizedWriteMessage(message, _defaultMessageWriter, cancellationToken)).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not TimeoutException && ex is not OperationCanceledException)
        {
            await CompletePipeWriterAsync(ex).ConfigureAwait(false);
            throw;
        }
        finally
        {
            if (!_writerCompleted)
                _defaultMessageWriter.Reset();
            _flushControl.Reset();
            _messageWriteLock.Release();
        }

    }

    public void WriteMessage<T>(T message, TimeSpan timeout = default) where T : IFrontendMessage
    {
        // TODO probably want to pass the remaining timeout to Unsynchronized.
        if (!_messageWriteLock.Wait(timeout))
            throw new TimeoutException("The operation has timed out.");

        _flushControl.Initialize(timeout);
        try
        {
            ProcessFlushResult(UnsynchronizedWriteMessage(message, _defaultMessageWriter).GetAwaiter().GetResult());
        }
        catch (Exception ex) when (ex is not TimeoutException)
        {
            CompletePipeWriter(ex);
            throw;
        }
        finally
        {
            if (!_writerCompleted)
                _defaultMessageWriter.Reset();
            _flushControl.Reset();
            _messageWriteLock.Release();
        }

    }

    ValueTask ProcessFlushResultAsync(FlushResult flushResult)
    {
        if (flushResult.IsCompleted)
            return CompletePipeWriterAsync();

        return new ValueTask();
    }

    void ProcessFlushResult(FlushResult flushResult)
    {
        if (flushResult.IsCompleted)
            CompletePipeWriter();
    }

    void CompletePipeWriter(Exception? exception = null)
    {
        _writerCompleted = true;
        _pipeWriter.Complete(exception);
    }

    ValueTask CompletePipeWriterAsync(Exception? exception = null)
    {
        _writerCompleted = true;
        return _pipeWriter.CompleteAsync(exception);
    }

    async ValueTask<FlushResult> UnsynchronizedWriteMessage<T, TWriter>(T message, MessageWriter<TWriter> writer, CancellationToken cancellationToken = default)
        where T : IFrontendMessage where TWriter : IBufferWriter<byte>
    {
        if (message is IStreamingFrontendMessage streamingMessage)
        {
            var result = await streamingMessage.WriteWithHeaderAsync(writer, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (result.IsCompleted)
                return result;
        } 
        else if (message.TryPrecomputeLength(out var precomputedLength))
        {
            if (precomputedLength < 0)
                throw new InvalidOperationException("TryPrecomputeLength out value \"length\" cannot be negative.");

            precomputedLength += MessageWriter.IntByteCount;
            writer.WriteByte((byte)message.FrontendCode);
            writer.WriteInt(precomputedLength);
            message.Write(writer);
            writer.Commit();
        }
        else
        {
            try
            {
                _headerBufferWriter ??= new MessageWriter<HeaderBufferWriter>(new HeaderBufferWriter());
                message.Write(_headerBufferWriter);
                _headerBufferWriter.Commit();
                // Completed then do something.
                _headerBufferWriter.Writer.SetCode((byte)message.FrontendCode);
                _headerBufferWriter.Writer.CopyTo(writer.Writer);
                writer.AdvanceCommitted(_headerBufferWriter.BytesCommitted);
            }
            finally
            {
                _headerBufferWriter!.Reset();
            }
        }

        // Flush all remaining data.
        if (writer.CanFlush)
            return await writer.ForceFlushAsync(cancellationToken).ConfigureAwait(false);

        return FlushResult.Default;
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
        var isAsync = cancellationToken.IsCancellationToken;
        // Take the smaller of the two.
        var readTimeout = isAsync && cancellationToken.Timeout != Timeout.InfiniteTimeSpan && cancellationToken.Timeout < _protocolOptions.ReadTimeout ? cancellationToken.Timeout : _protocolOptions.ReadTimeout;
        var start = isAsync ? -1 : TickCount64Shim.Get();
        do
        {
            var buffer = isAsync
                ? await ReadAsync(ComputeMinimumSize(resumptionData), cancellationToken.CancellationToken).ConfigureAwait(false)
                : Read(ComputeMinimumSize(resumptionData), readTimeout);

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
                    await CompletePipeReader(isAsync, exception).ConfigureAwait(false);
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

            if (start != -1 && status != ReadStatus.Done)
            {
                var elapsed = TimeSpan.FromMilliseconds(TickCount64Shim.Get() - start);
                readTimeout = readTimeout - elapsed < _protocolOptions.ReadTimeout ? elapsed : _protocolOptions.ReadTimeout;
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
                await _reader.CompleteAsync(exception).ConfigureAwait(false);
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
            return remainingMessage < _protocolOptions.ReaderSegmentSize ? remainingMessage : _protocolOptions.ReaderSegmentSize;
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

        async ValueTask<ReadOnlySequence<byte>> ReadAsync(int minimumSize, CancellationToken cancellationToken)
        {
            if (!_reader.TryRead(minimumSize, out var buffer))
            {
                // TODO should be cached.
                var timeoutSource = new CancellationTokenSource();
                using var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(timeoutSource.Token, cancellationToken);
                timeoutSource.CancelAfter(_protocolOptions.ReadTimeout);
                try
                {
                    return await _reader.ReadAtLeastAsync(minimumSize, cancellationTokenSource.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException ex) when (timeoutSource.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                {
                    throw new TimeoutException("The operation has timed out.", ex);
                }
            }

            return buffer;
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

            return conn;
        }
        catch (Exception ex)
        {
            conn.Dispose();
            throw;
        }
    }

    public static ValueTask<PgV3Protocol> StartAsync(PipeWriter writer, PipeReader reader, PgOptions options, ProtocolOptions? pipeOptions = null)
    {
        var conn = new PgV3Protocol(writer, reader, pipeOptions);
        return StartAsyncCore(conn, options);
    }

    public static ValueTask<PgV3Protocol> StartAsync(IPipeWriterSyncSupport writer, IPipeReaderSyncSupport reader, PgOptions options, ProtocolOptions? pipeOptions = null)
    {
        var conn = new PgV3Protocol(writer, reader, pipeOptions);
        return StartAsyncCore(conn, options);
    }

    public static PgV3Protocol Start(IPipeWriterSyncSupport writer, IPipeReaderSyncSupport reader, PgOptions options, ProtocolOptions? pipeOptions = null)
    {
        try
        {
            var conn = new PgV3Protocol(writer, reader, pipeOptions);
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

            return conn;
        }
        catch (Exception ex)
        {
            writer.PipeWriter.Complete(ex);
            reader.PipeReader.Complete(ex);
            throw;
        }
    }

    public async ValueTask ExecuteQueryAsync(string commandText, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> parameters, string? preparedStatementName = null, TimeSpan commandTimeout = default, CancellationToken cancellationToken = default)
    {
        if (!_messageWriteLock.Wait(0))
            await _messageWriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        var writer = _defaultMessageWriter;
        _flushControl.Initialize();
        _flushControl.AlwaysObserveFlushThreshold = true;
        try
        {
            var portal = string.Empty;
            await WriteMessage(new Parse(commandText, parameters, preparedStatementName)).ConfigureAwait(false);
            await WriteMessage(new Bind(portal, parameters, ResultColumnCodes.CreateOverall(FormatCode.Binary))).ConfigureAwait(false);
            await WriteMessage(new Describe(DescribeName.CreateForPortal(portal))).ConfigureAwait(false);
            await WriteMessage(new Execute(portal)).ConfigureAwait(false);
            await WriteMessage(new Sync()).ConfigureAwait(false);

            // Disable extraneous flush suppression and truly flush all data.
            _flushControl.AlwaysObserveFlushThreshold = false;
            await _flushControl.FlushAsync(observeFlushThreshold: false, cancellationToken);
        }
        catch (Exception ex) when (ex is not TimeoutException && ex is not OperationCanceledException)
        {
            await CompletePipeWriterAsync(ex).ConfigureAwait(false);
            throw;
        }
        finally
        {
            if (!_writerCompleted)
                _defaultMessageWriter.Reset();
            _flushControl.Reset();
            _messageWriteLock.Release();
        }


        using var commandTimeoutSource = new CancellationTokenSource();
        commandTimeoutSource.CancelAfter(commandTimeout != TimeSpan.Zero ? commandTimeout : _protocolOptions.CommandTimeout);
        await ReadMessageAsync<ParseComplete>(commandTimeoutSource.Token).ConfigureAwait(false);
        await ReadMessageAsync<BindComplete>().ConfigureAwait(false);
        var description = await ReadMessageAsync(new RowDescription(_fieldDescriptionPool)).ConfigureAwait(false);

        var dataReader = new DataReader(this, description);
        while (await dataReader.ReadAsync().ConfigureAwait(false))
        {
            var i = await dataReader.GetFieldValueAsync<int>().ConfigureAwait(false);;
            i = i;
        }

        // TODO meh
        async ValueTask WriteMessage<T>(T message) where T : IFrontendMessage
        {
            await ProcessFlushResultAsync(await UnsynchronizedWriteMessage(message, writer, cancellationToken).ConfigureAwait(false)).ConfigureAwait(false);
        }
    }

    public async ValueTask ExecutePreparedQueryAsync(string preparedStatementName, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> parameters, TimeSpan commandTimeout = default)
    {
        var portal = string.Empty;
        await WriteMessageAsync(new Bind(portal, parameters, ResultColumnCodes.CreateOverall(FormatCode.Binary), preparedStatementName)).ConfigureAwait(false);
        await WriteMessageAsync(new Execute(portal)).ConfigureAwait(false);
        await WriteMessageAsync(new Sync()).ConfigureAwait(false);

        using var commandTimeoutSource = new CancellationTokenSource();
        commandTimeoutSource.CancelAfter(commandTimeout != TimeSpan.Zero ? commandTimeout : _protocolOptions.CommandTimeout);
        await ReadMessageAsync<BindComplete>(commandTimeoutSource.Token).ConfigureAwait(false);

        // var dataReader = new DataReader(conn, description);
        // while (await dataReader.ReadAsync())
        // {
        //     var i = await dataReader.GetFieldValueAsync<int>();
        //     i = i;
        // }
    }

    public void ExecuteQuery(string commandText, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> parameters, string? preparedStatementName = null, TimeSpan commandTimeout = default)
    {
        var portal = string.Empty;
        WriteMessage(new Parse(commandText, parameters, preparedStatementName));
        WriteMessage(new Bind(portal, parameters, ResultColumnCodes.CreateOverall(FormatCode.Binary)));
        WriteMessage(new Describe(DescribeName.CreateForPortal(portal)));
        WriteMessage(new Execute(portal));
        WriteMessage(new Sync());
        ReadMessage<ParseComplete>(commandTimeout != TimeSpan.Zero ? commandTimeout : _protocolOptions.CommandTimeout);
        ReadMessage<BindComplete>();
        var description = ReadMessage(new RowDescription(_fieldDescriptionPool));

        // var dataReader = new DataReader(conn, description);
        // while (await dataReader.ReadAsync())
        // {
        //     var i = await dataReader.GetFieldValueAsync<int>();
        //     i = i;
        // }
    }

    public void ExecutePreparedQuery(string preparedStatementName, ArraySegment<KeyValuePair<CommandParameter, IParameterWriter>> parameters, TimeSpan commandTimeout = default)
    {
        var portal = string.Empty;
        WriteMessage(new Bind(portal, parameters, ResultColumnCodes.CreateOverall(FormatCode.Binary), preparedStatementName));
        WriteMessage(new Execute(portal));
        WriteMessage(new Sync());
        ReadMessage<BindComplete>(commandTimeout != TimeSpan.Zero ? commandTimeout : _protocolOptions.CommandTimeout);

        // var dataReader = new DataReader(conn, description);
        // while (await dataReader.ReadAsync())
        // {
        //     var i = await dataReader.GetFieldValueAsync<int>();
        //     i = i;
        // }
    }

    public void Dispose()
    {
        _reader.Complete();
        _pipeWriter.Complete();
        _messageWriteLock.Dispose();
        _flushControl.Dispose();
    }
}



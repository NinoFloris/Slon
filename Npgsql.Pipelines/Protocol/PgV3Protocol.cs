using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Npgsql.Pipelines.Buffers;
using FlushResult = Npgsql.Pipelines.Buffers.FlushResult;

namespace Npgsql.Pipelines.Protocol;

record ProtocolOptions
{
    public TimeSpan ReadTimeout { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan WriteTimeout { get; init;  } = TimeSpan.FromSeconds(1);
    /// <summary>
    /// CommandTimeout affects the first IO read after writing out a command.
    /// Default is infinite, where behavior purely relies on read and write timeouts.
    /// </summary>
    public TimeSpan CommandTimeout { get; init; } = Timeout.InfiniteTimeSpan;
    public int ReaderSegmentSize { get; init; } = 8192;
    public int WriterSegmentSize { get; init; } = 8192;

    public int MaximumMessageChunkSize { get; init; } = 8192 / 2;
    public int FlushThreshold { get; init; } = 8192 / 2;

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

    // Lock held for a message write, writes to the pipe for one message shouldn't be interleaved with another.
    readonly SemaphoreSlim _messageWriteLock = new(1);
    readonly ResettableFlushControl _flushControl;
    readonly MessageWriter<IPipeWriterSyncSupport> _defaultMessageWriter;
    bool _pipesCompleted;

    readonly Queue<OperationSource> _pending = new();

    readonly Action<OperationSource> _completeOperationAction;
    readonly OperationSource _readyOperationSource;
    readonly CommandReader _commandReaderSingleton;
    readonly CommandWriter _commandWriterSingleton;
    readonly RowDescription _rowDescriptionSingleton;

    PgV3Protocol(IPipeWriterSyncSupport writer, IPipeReaderSyncSupport reader, ProtocolOptions? protocolOptions = null)
    {
        _protocolOptions = protocolOptions ?? DefaultPipeOptions;
        _pipeWriter = writer.PipeWriter;
        _flushControl = new ResettableFlushControl(writer, _protocolOptions.WriteTimeout, Math.Max(BufferWriter.DefaultCommitThreshold, _protocolOptions.FlushThreshold));
        _defaultMessageWriter = new MessageWriter<IPipeWriterSyncSupport>(writer, _flushControl);
        _reader = new SimplePipeReader(reader, _protocolOptions.ReadTimeout);
        _completeOperationAction = operation => CompleteOperation(operation);
        _readyOperationSource = new OperationSource(pooled: true);
        _readyOperationSource.Activate(this, _completeOperationAction);
        _commandReaderSingleton = new();
        _commandWriterSingleton = new(this);
        _rowDescriptionSingleton = new();
    }

    PgV3Protocol(PipeWriter writer, PipeReader reader, ProtocolOptions? protocolOptions = null)
        : this(new AsyncOnlyPipeWriter(writer), new AsyncOnlyPipeReader(reader), protocolOptions)
    { }

    // TODO add some ownership transfer logic here, allocating new instances if the singleton isn't back yet.
    public CommandReader GetCommandReader() => _commandReaderSingleton;
    public CommandWriter GetCommandWriter() => _commandWriterSingleton;
    public RowDescription GetRowDescription() => _rowDescriptionSingleton;

    public IOCompletionPair WriteMessageAsync<T>(T message, CancellationToken cancellationToken = default) where T : IFrontendMessage
    {
        OperationSource operationSource;
        lock (_pending)
        {
            operationSource = _pending.Count == 0 ? _readyOperationSource : new OperationSource();
            _pending.Enqueue(operationSource);
        }

        return new IOCompletionPair(Core(this, message, cancellationToken), operationSource.Task);

        static async ValueTask Core(PgV3Protocol instance, T message, CancellationToken cancellationToken)
        {
            if (!instance._messageWriteLock.Wait(0))
                await instance._messageWriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

            instance._flushControl.Initialize();
            try
            {
                await UnsynchronizedWriteMessage(instance._defaultMessageWriter, message, cancellationToken).ConfigureAwait(false);
                if (instance._flushControl.WriterCompleted)
                    await instance.CompletePipesAsync().ConfigureAwait(false);
                else
                    await instance._defaultMessageWriter.FlushAsync(observeFlushThreshold: false, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not TimeoutException && ex is not OperationCanceledException)
            {
                await instance.CompletePipesAsync(ex).ConfigureAwait(false);
                throw;
            }
            finally
            {
                if (!instance._pipesCompleted)
                {
                    instance._defaultMessageWriter.Reset();
                    instance._flushControl.Reset();
                }
                instance._messageWriteLock.Release();
            }

        }
    }

    public readonly struct BatchWriter
    {
        readonly PgV3Protocol _protocol;

        internal BatchWriter(PgV3Protocol protocol)
        {
            _protocol = protocol;
        }

        public ValueTask WriteMessageAsync<T>(T message, CancellationToken cancellationToken = default) where T : IFrontendMessage
            => UnsynchronizedWriteMessage(_protocol._defaultMessageWriter, message, cancellationToken);

        public long UnflushedBytes => _protocol._defaultMessageWriter.UnflushedBytes;
        public ValueTask<FlushResult> FlushAsync(bool observeFlushThreshold = true, CancellationToken cancellationToken = default)
            => _protocol._flushControl.FlushAsync(observeFlushThreshold, cancellationToken: cancellationToken);
    }

    public IOCompletionPair WriteMessageBatchAsync<TState>(Func<BatchWriter, TState, CancellationToken, ValueTask> batchWriter, TState state, bool flushHint = true, CancellationToken cancellationToken = default)
    {
        OperationSource operationSource;
        lock (_pending)
        {
            operationSource = _pending.Count == 0 ? _readyOperationSource : new OperationSource();
            _pending.Enqueue(operationSource);
        }

        return new IOCompletionPair(Core(this, batchWriter, state, flushHint, cancellationToken), operationSource.Task);

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
        static async ValueTask Core(PgV3Protocol instance, Func<BatchWriter, TState, CancellationToken, ValueTask> batchWriter, TState state, bool flushHint = true, CancellationToken cancellationToken = default)
        {
            if (!instance._messageWriteLock.Wait(0))
                await instance._messageWriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

            instance._flushControl.Initialize();
            try
            {
                await batchWriter.Invoke(new BatchWriter(instance), state, cancellationToken).ConfigureAwait(false);
                if (instance._flushControl.WriterCompleted)
                    await instance.CompletePipesAsync().ConfigureAwait(false);
                else if (flushHint)
                    await instance._defaultMessageWriter.FlushAsync(observeFlushThreshold: false, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not TimeoutException && (ex is not OperationCanceledException || ex is OperationCanceledException oce && oce.CancellationToken != cancellationToken))
            {
                await instance.CompletePipesAsync(ex).ConfigureAwait(false);
                throw;
            }
            finally
            {
                if (!instance._pipesCompleted)
                {
                    instance._defaultMessageWriter.Reset();
                    instance._flushControl.Reset();
                }
                instance._messageWriteLock.Release();
            }
        }
    }

    void CompleteOperation(OperationSource operationSource)
    {
        OperationSource queuedSource;
        bool hasNext = false;
        lock (_pending)
        {
            if (_pending.TryPeek(out queuedSource!) && ReferenceEquals(queuedSource, operationSource))
            {
                _pending.Dequeue();
                while ((hasNext = _pending.TryPeek(out queuedSource!)) && !queuedSource.Completed)
                {}
            }
        }
        // Activate the next uncompleted one, outside the lock.
        if (hasNext)
            queuedSource.Activate(this, _completeOperationAction);
    }

    public void WriteMessage<T>(T message, TimeSpan timeout = default) where T : IFrontendMessage
    {
        // TODO probably want to pass the remaining timeout to flush control.
        if (!_messageWriteLock.Wait(timeout))
            throw new TimeoutException("The operation has timed out.");

        _flushControl.InitializeAsBlocking(timeout);
        try
        {
            UnsynchronizedWriteMessage(_defaultMessageWriter, message).GetAwaiter().GetResult();
            if (_flushControl.WriterCompleted)
                CompletePipes();
            else
                _defaultMessageWriter.FlushAsync(observeFlushThreshold: false).GetAwaiter().GetResult();
        }
        catch (Exception ex) when (ex is not TimeoutException)
        {
            CompletePipes(ex);
            throw;
        }
        finally
        {
            if (!_pipesCompleted)
            {
                _defaultMessageWriter.Reset();
                _flushControl.Reset();
            }
            _messageWriteLock.Release();
        }

    }

    void CompletePipes(Exception? exception = null)
    {
        _pipesCompleted = true;
        _pipeWriter.Complete(exception);
        _reader.Complete(exception);
    }

    async ValueTask CompletePipesAsync(Exception? exception = null)
    {
        _pipesCompleted = true;
        await _pipeWriter.CompleteAsync(exception);
        await _reader.CompleteAsync(exception);
    }

    static ValueTask UnsynchronizedWriteMessage<TWriter, T>(MessageWriter<TWriter> writer, T message, CancellationToken cancellationToken = default)
        where TWriter : IBufferWriter<byte> where T : IFrontendMessage
    {
        if (message.TryPrecomputeLength(out var precomputedLength))
        {
            if (precomputedLength < 0)
                ThrowOutOfRangeException();

            writer.Writer.Ensure(precomputedLength + MessageHeader.ByteCount);
            precomputedLength += MessageWriter.IntByteCount;
            writer.WriteByte((byte)message.FrontendCode);
            writer.WriteInt(precomputedLength);
            message.Write(ref writer.Writer);
            writer.Writer.Commit();
        }
        else if (message is IStreamingFrontendMessage streamingMessage)
        {
            var result =  streamingMessage.WriteWithHeaderAsync(writer, cancellationToken);
            return result.IsCompletedSuccessfully ? new ValueTask() : new ValueTask(result.AsTask());
        }
        else
        {
            WriteBufferedMessage(message, new HeaderBufferWriter(), writer);
        }

        return new ValueTask();

        static void ThrowOutOfRangeException() =>
            throw new ArgumentOutOfRangeException(nameof(precomputedLength), "TryPrecomputeLength out value \"length\" cannot be negative.");

        static void WriteBufferedMessage(T message, HeaderBufferWriter headerBufferWriter, MessageWriter<TWriter> writer)
        {
            try
            {
                var bufferWriter = new BufferWriter<HeaderBufferWriter>(headerBufferWriter);
                message.Write(ref bufferWriter);
                bufferWriter.Output.SetCode((byte)message.FrontendCode);
                bufferWriter.CopyTo(ref writer.Writer);
                writer.Writer.Commit();
            }
            finally
            {
                headerBufferWriter.Reset();
            }
        }
    }

    public async ValueTask WaitForDataAsync(int minimumSize, CancellationToken cancellationToken = default)
    {
        await _reader.ReadAtLeastAsync(minimumSize, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<T> ReadMessageAsync<T>(T message, CancellationToken cancellationToken = default) where T : IBackendMessage => Reader.ReadAsync(this, message, cancellationToken);
    public ValueTask<T> ReadMessageAsync<T>(CancellationToken cancellationToken = default) where T : struct, IBackendMessage => Reader.ReadAsync(this, new T(), cancellationToken);
    public T ReadMessage<T>(T message, TimeSpan timeout = default) where T : IBackendMessage => Reader.Read(this, message, timeout);
    public T ReadMessage<T>(TimeSpan timeout = default) where T : struct, IBackendMessage => Reader.Read(this, new T(), timeout);
    static class Reader
    {
        // As MessageReader is a ref struct we need a small method to create it and pass a reference for the async versions.
        static ReadStatus ReadCore<TMessage>(ref TMessage message, in ReadOnlySequence<byte> sequence, ref MessageReader.ResumptionData resumptionData, ref long consumed) where TMessage: IBackendMessage
        {
            scoped MessageReader reader;
            if (resumptionData.IsDefault)
            {
                reader = MessageReader.Create(sequence);
                if (consumed != 0)
                    reader.Advance(consumed);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static int ComputeMinimumSize(long consumed, in MessageReader.ResumptionData resumptionData, int maximumMessageChunk)
        {
            uint minimumSize = MessageHeader.ByteCount;
            uint remainingMessage;
            // If we're in a message but it's consumed we assume the reader wants to read the next header.
            // Otherwise we'll return either remainingMessage or maximumMessageChunk, whichever is smaller.
            if (!resumptionData.IsDefault && (remainingMessage = resumptionData.Header.Length - resumptionData.MessageIndex) > 0)
                minimumSize = remainingMessage < maximumMessageChunk ? remainingMessage : Unsafe.As<int, uint>(ref maximumMessageChunk);

            var result = consumed + minimumSize;
            if (result > int.MaxValue)
                ThrowOverflowException();

            return Unsafe.As<long, int>(ref result);

            static void ThrowOverflowException() => throw new OverflowException("Buffers cannot be larger than int.MaxValue, return ReadStatus.ConsumeData to free data while processing.");
        }

        static Exception CreateUnexpectedError<T>(ReadOnlySequence<byte> buffer, scoped in MessageReader.ResumptionData resumptionData, long consumed, Exception? readerException = null)
        {
            // Try to read error response.
            Exception exception;
            if (readerException is null && resumptionData.IsDefault == false && resumptionData.Header.Code == BackendCode.ErrorResponse)
            {
                var errorResponse = new ErrorResponse();
                Debug.Assert(resumptionData.MessageIndex <= int.MaxValue);
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
                exception = new Exception($"Protocol desync on message: {typeof(T).FullName}, expected different response{(resumptionData.Header.IsDefault ? "" : ", actual code: " + resumptionData.Header.Code)}.", readerException);
            }
            return exception;
        }

        static ValueTask HandleAsyncResponse(in ReadOnlySequence<byte> buffer, scoped ref MessageReader.ResumptionData resumptionData, ref long consumed)
        {
            // switch (asyncResponseStatus)
            // {
            //     case ReadStatus.AsyncResponse:
            //         throw new Exception("Should never happen, async response handling should not return ReadStatus.AsyncResponse.");
            //     case ReadStatus.InvalidData:
            //         throw new Exception("Should never happen, any unknown data during async response handling should be left for the original message handler.");
            //     case ReadStatus.NeedMoreData:
            //         _reader.Advance(consumed);
            //         consumed = 0;
            //         buffer = isAsync
            //             ? await ReadAsync(ComputeMinimumSize(resumptionData), cancellationToken.CancellationToken).ConfigureAwait(false)
            //             : Read(ComputeMinimumSize(resumptionData), cancellationToken.Timeout);
            //         break;
            //     case ReadStatus.Done:
            //         // We don't reset consumed here, the original handler may continue where we left.
            //         break;
            // }
            //
            // void HandleAsyncResponseCore
            //
            var reader = consumed == 0 ? MessageReader.Resume(buffer, resumptionData) : MessageReader.Create(buffer, resumptionData, consumed);

            consumed = (int)reader.Consumed;
            throw new NotImplementedException();
        }

        public static T Read<T>(PgV3Protocol protocol, T message, TimeSpan timeout = default) where T : IBackendMessage
        {
            ReadStatus status;
            MessageReader.ResumptionData resumptionData = default;
            long consumed = 0;
            Exception? readerExn = null;
            var readTimeout = timeout != Timeout.InfiniteTimeSpan ? timeout : protocol._protocolOptions.ReadTimeout;
            var start = TickCount64Shim.Get();
            do
            {
                var buffer =protocol._reader.ReadAtLeast(ComputeMinimumSize(consumed, resumptionData, protocol._protocolOptions.MaximumMessageChunkSize), readTimeout);

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
                    case ReadStatus.ConsumeData:
                        protocol._reader.Advance(consumed);
                        consumed = 0;
                        break;
                    case ReadStatus.NeedMoreData:
                        break;
                    case ReadStatus.InvalidData:
                        var exception = CreateUnexpectedError<T>(buffer, resumptionData, consumed, readerExn);
                        protocol.CompletePipes(exception);
                        throw exception;
                    case ReadStatus.AsyncResponse:
                        protocol._reader.Advance(consumed);
                        consumed = 0;
                        HandleAsyncResponse(buffer, ref resumptionData, ref consumed).GetAwaiter().GetResult();
                        break;
                }

                if (start != -1 && status != ReadStatus.Done)
                {
                    var elapsed = TimeSpan.FromMilliseconds(TickCount64Shim.Get() - start);
                    readTimeout -= elapsed;
                }
            } while (status != ReadStatus.Done);

            return message;
        }

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        public static async ValueTask<T> ReadAsync<T>(PgV3Protocol protocol, T message, CancellationToken cancellationToken = default) where T : IBackendMessage
        {
            ReadStatus status;
            MessageReader.ResumptionData resumptionData = default;
            long consumed = 0;
            Exception? readerExn = null;
            do
            {
                var buffer = await protocol._reader.ReadAtLeastAsync(ComputeMinimumSize(consumed, resumptionData, protocol._protocolOptions.MaximumMessageChunkSize), cancellationToken);

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
                    case ReadStatus.ConsumeData:
                        protocol._reader.Advance(consumed);
                        consumed = 0;
                        break;
                    case ReadStatus.NeedMoreData:
                        break;
                    case ReadStatus.InvalidData:
                        var exception = CreateUnexpectedError<T>(buffer, resumptionData, consumed, readerExn);
                        await protocol.CompletePipesAsync(exception);
                        throw exception;
                    case ReadStatus.AsyncResponse:
                        protocol._reader.Advance(consumed);
                        consumed = 0;
                        await HandleAsyncResponse(buffer, ref resumptionData, ref consumed);
                        break;
                }
            } while (status != ReadStatus.Done);

            return message;
        }
    }

    static async ValueTask<PgV3Protocol> StartAsyncCore(PgV3Protocol conn, PgOptions options)
    {
        try
        {
            var completionPair = conn.WriteMessageAsync(new StartupRequest(options));
            var op = await completionPair.SelectAsync().ConfigureAwait(false);
            var msg = await op.Protocol.ReadMessageAsync(new AuthenticationResponse()).ConfigureAwait(false);
            switch (msg.AuthenticationType)
            {
                case AuthenticationType.Ok:
                    await op.Protocol.ReadMessageAsync<StartupResponse>().ConfigureAwait(false);
                    op.Complete();
                    break;
                case AuthenticationType.MD5Password:
                    if (options.Password is null)
                        throw new InvalidOperationException("No password given, connection expects password.");
                    completionPair = op.Protocol.WriteMessageAsync(new PasswordMessage(options.Username, options.Password, msg.MD5Salt));
                    op = await completionPair.SelectAsync().ConfigureAwait(false);
                    var expectOk = await op.Protocol.ReadMessageAsync(new AuthenticationResponse()).ConfigureAwait(false);
                    if (expectOk.AuthenticationType != AuthenticationType.Ok)
                        throw new Exception("Unexpected authentication response");
                    await op.Protocol.ReadMessageAsync<StartupResponse>().ConfigureAwait(false);
                    op.Complete();
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

    public void Dispose()
    {
        _reader.Complete();
        _pipeWriter.Complete();
        _messageWriteLock.Dispose();
        _flushControl.Dispose();
    }
}

// TODO this should be able to be marked as exclusive use (which will not require more enqueues on presentation of the same source)
// that will also allow us to easily sort the pool by conn.Pending < conn2.Pending && !conn.PendingHead?.Exclusive.
// TODO probably have to nicely integrate cancellation for when we have to drain the pipeline.
sealed class OperationSource: IValueTaskSource<Operation>
{
    readonly bool _pooled;
    Action<OperationSource>? _completeAction;
    ManualResetValueTaskSourceCore<Operation> _tcs; // mutable struct; do not make this readonly

    public bool Completed => _completeAction is null;

    public OperationSource(bool pooled = false)
    {
        _pooled = pooled;
        // TODO the optimal setting can be derived from knowledge we have, if we're on the same DbConnection
        // we can assume pipelining (so false is safe), for multiplexing we should do true unless it can prove the same thing (same DbConnection && being enqueued consecutively).
        _tcs = new ManualResetValueTaskSourceCore<Operation> { RunContinuationsAsynchronously = true };
    }

    public ValueTask<Operation> Task => _pooled
        ? new ValueTask<Operation>(_tcs.GetResult(_tcs.Version))
        : new(this, _tcs.Version);

    public void Activate(PgV3Protocol protocol, Action<OperationSource> completeAction)
    {
        _completeAction = completeAction;
        _tcs.SetResult(new Operation(this, protocol, _tcs.Version));
    }

    public void Complete(short token)
    {
        var completeAction = _completeAction;
        // Try to prevent exceptions coming from _completeAction, PgV3ProtocolOperation should be tolerating of double disposes etc.
        if (completeAction is null || token != _tcs.Version)
            return;

        if (!_pooled)
        {
            _completeAction = null;
        }
        // else
        // {
        //     var result = _tcs.GetResult(token);
        //     _tcs.Reset();
        //     _tcs.SetResult(new PgV3ProtocolOperation(this, result.Protocol, _tcs.Version));
        // }
        completeAction(this);
    }

    Operation IValueTaskSource<Operation>.GetResult(short token) => _tcs.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<Operation>.GetStatus(short token) => _tcs.GetStatus(token);
    void IValueTaskSource<Operation>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _tcs.OnCompleted(continuation, state, token, flags);
}

readonly struct Operation: IDisposable
{
    readonly OperationSource _source;
    readonly short _token;

    internal Operation(OperationSource source, PgV3Protocol protocol, short token)
    {
        _source = source;
        _token = token;
        Protocol = protocol;
    }

    public PgV3Protocol Protocol { get; }

    public void Complete() => _source?.Complete(_token);
    public void Dispose() => Complete();
}

readonly struct IOCompletionPair
{
    public IOCompletionPair(ValueTask write, ValueTask<Operation> read)
    {
        Write = write.Preserve();
        Read = read.Preserve();
    }

    public ValueTask Write { get; }
    public ValueTask<Operation> Read { get; }

    /// <summary>
    /// Checks whether Write or Read is completed (in that order) before waiting on either for one to complete.
    /// If Read is completed we don't wait for Write anymore but we will check its status on future invocations.
    /// </summary>
    /// <returns></returns>
    public ValueTask<Operation> SelectAsync()
    {
        if (Write.IsCompletedSuccessfully || (!Write.IsCompleted && Read.IsCompleted))
            return Read;

        if (Write.IsFaulted || Write.IsCanceled)
        {
            Write.GetAwaiter().GetResult();
            return default;
        }

        return Core(this);

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        static async ValueTask<Operation> Core(IOCompletionPair instance)
        {
            await Task.WhenAny(instance.Write.AsTask(), instance.Read.AsTask());
            if (instance.Write.IsCompletedSuccessfully || (!instance.Write.IsCompleted && instance.Read.IsCompleted))
                return await instance.Read;

            if (instance.Write.IsFaulted || instance.Write.IsCanceled)
            {
                instance.Write.GetAwaiter().GetResult();
                return default;
            }

            throw new InvalidOperationException("Should not get here");
        }
    }
}

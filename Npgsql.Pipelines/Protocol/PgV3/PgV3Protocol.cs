using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;
using FlushResult = Npgsql.Pipelines.Buffers.FlushResult;

namespace Npgsql.Pipelines.Protocol.PgV3;

record PgV3ProtocolOptions
{
    public TimeSpan ReadTimeout { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan WriteTimeout { get; init;  } = TimeSpan.FromSeconds(1);
    public int ReaderSegmentSize { get; init; } = 8192;
    public int WriterSegmentSize { get; init; } = 8192;
    public int MaximumMessageChunkSize { get; init; } = 8192 / 2;
    public int FlushThreshold { get; init; } = 8192 / 2;
}

class PgV3Protocol : PgProtocol
{
    static PgV3ProtocolOptions DefaultPipeOptions { get; } = new();
    readonly PgV3ProtocolOptions _protocolOptions;
    readonly SimplePipeReader _reader;
    readonly PipeWriter _pipeWriter;

    readonly ResettableFlushControl _flushControl;
    readonly MessageWriter<IPipeWriterSyncSupport> _defaultMessageWriter;

    // Lock held for the duration of an individual message write or an entire exclusive use.
    readonly SemaphoreSlim _messageWriteLock = new(1);
    readonly Queue<PgV3OperationSource> _operations;

    readonly PgV3OperationSource _operationSourceSingleton;
    readonly PgV3OperationSource _exclusiveOperationSourceSingleton;
    readonly CommandReader _commandReaderSingleton;
    readonly RowDescription _rowDescriptionSingleton;

    volatile PgProtocolState _state = PgProtocolState.Created;
    volatile bool _disposed;
    volatile Exception? _completingException;
    volatile int _pendingExclusiveUses;

    PgV3Protocol(IPipeWriterSyncSupport writer, IPipeReaderSyncSupport reader, PgV3ProtocolOptions? protocolOptions = null)
    {
        _protocolOptions = protocolOptions ?? DefaultPipeOptions;
        _pipeWriter = writer.PipeWriter;
        _flushControl = new ResettableFlushControl(writer, _protocolOptions.WriteTimeout, Math.Max(BufferWriter.DefaultCommitThreshold, _protocolOptions.FlushThreshold));
        _defaultMessageWriter = new MessageWriter<IPipeWriterSyncSupport>(writer, _flushControl);
        _reader = new SimplePipeReader(reader, _protocolOptions.ReadTimeout);
        _operations = new();
        _operationSourceSingleton = new PgV3OperationSource(this, exclusiveUse: false, pooled: true);
        _exclusiveOperationSourceSingleton = new PgV3OperationSource(this, exclusiveUse: true, pooled: true);
        _commandReaderSingleton = new();
        _rowDescriptionSingleton = new();
    }

    PgV3Protocol(PipeWriter writer, PipeReader reader, PgV3ProtocolOptions? protocolOptions = null)
        : this(new AsyncOnlyPipeWriter(writer), new AsyncOnlyPipeReader(reader), protocolOptions)
    { }

    object InstanceToken => this;
    object SyncObj => _operations;

    bool IsCompleted => _state is PgProtocolState.Completed;

    void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(PgV3Protocol));
    }

    PgV3OperationSource ThrowIfInvalidSlot(OperationSlot slot, bool allowUnbound = false, bool allowActivated = true)
    {
        if (slot is not PgV3OperationSource source)
            throw new ArgumentException("Cannot accept this type of slot.", nameof(slot));

        if (!allowUnbound && source.IsUnbound)
            throw new ArgumentException("Cannot accept an unbound slot.", nameof(slot));

        if (!allowUnbound && !ReferenceEquals(source.InstanceToken, InstanceToken))
            throw new ArgumentException("Cannot accept a slot for some other connection.", nameof(slot));

        if (source.IsCompleted || (!allowActivated && source.IsActivated))
            throw new ArgumentException("Cannot accept a completed operation.", nameof(slot));

        return source;

    }

    public override async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (_pipeWriter.UnflushedBytes == 0)
            return;

        if (!_messageWriteLock.Wait(0))
            await _messageWriteLock.WaitAsync(cancellationToken);

        _flushControl.Initialize();
        try
        {
            await _flushControl.FlushAsync(observeFlushThreshold: false, cancellationToken: cancellationToken);
        }
        catch (Exception ex) when (ex is not TimeoutException && (ex is not OperationCanceledException || ex is OperationCanceledException oce && oce.CancellationToken != cancellationToken))
        {
            MoveToComplete(ex);
            throw;
        }
        finally
        {
            if (!IsCompleted)
            {
                // We must reset the writer as it holds onto an output segment that is now flushed.
                _defaultMessageWriter.Reset();
                _flushControl.Reset();
            }

            if (!_disposed)
                _messageWriteLock.Release();
        }
    }

    // TODO maybe add some ownership transfer logic here, allocating new instances if the singleton isn't back yet.
    public override CommandReader GetCommandReader() => _commandReaderSingleton;
    public RowDescription GetRowDescription() => _rowDescriptionSingleton;

    public IOCompletionPair WriteMessageAsync<T>(OperationSlot slot, T message, CancellationToken cancellationToken = default) where T : IFrontendMessage<PgV3FrontendHeader>
        => WriteMessageBatchAsync(slot, (batchWriter, message, cancellationToken) => batchWriter.WriteMessageAsync(message, cancellationToken), message, flushHint: true, cancellationToken);

    public readonly struct BatchWriter
    {
        readonly PgV3Protocol _protocol;

        internal BatchWriter(PgV3Protocol protocol)
        {
            _protocol = protocol;
        }

        public ValueTask WriteMessageAsync<T>(T message, CancellationToken cancellationToken = default) where T : IFrontendMessage<PgV3FrontendHeader>
            => UnsynchronizedWriteMessage(_protocol._defaultMessageWriter, message, cancellationToken);

        public long UnflushedBytes => _protocol._defaultMessageWriter.UnflushedBytes;
        public ValueTask<FlushResult> FlushAsync(bool observeFlushThreshold = true, CancellationToken cancellationToken = default)
            => _protocol._flushControl.FlushAsync(observeFlushThreshold, cancellationToken: cancellationToken);
    }

    public IOCompletionPair WriteMessageBatchAsync<TState>(OperationSlot slot, Func<BatchWriter, TState, CancellationToken, ValueTask> batchWriter, TState state, bool flushHint = true, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var source = ThrowIfInvalidSlot(slot);

        return new IOCompletionPair(Core(this, source, batchWriter, state, flushHint, cancellationToken), slot.Task);

#if !NETSTANDARD2_0
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        static async ValueTask<WriteResult> Core(PgV3Protocol instance, PgV3OperationSource source, Func<BatchWriter, TState, CancellationToken, ValueTask> batchWriter, TState state, bool flushHint = true, CancellationToken cancellationToken = default)
        {
            await source.WriteSlot;

            instance._flushControl.Initialize();
            try
            {
                await batchWriter.Invoke(new BatchWriter(instance), state, cancellationToken).ConfigureAwait(false);
                if (instance._flushControl.WriterCompleted)
                    instance.MoveToComplete();
                else if (flushHint)
                    await instance._defaultMessageWriter.FlushAsync(observeFlushThreshold: false, cancellationToken).ConfigureAwait(false);

                return new WriteResult(instance._defaultMessageWriter.BytesCommitted);
            }
            catch (Exception ex) when (ex is not TimeoutException && (ex is not OperationCanceledException || ex is OperationCanceledException oce && oce.CancellationToken != cancellationToken))
            {
                instance.MoveToComplete(ex);
                throw;
            }
            finally
            {
                if (!instance.IsCompleted)
                {
                    instance._defaultMessageWriter.Reset();
                    instance._flushControl.Reset();
                }

                if (!instance._disposed)
                    source.EndWrites(instance._messageWriteLock);
            }
        }
    }


    // TODO this is not up-to-date with the async implementation.
    public void WriteMessage<T>(T message, TimeSpan timeout = default) where T : IFrontendMessage<PgV3FrontendHeader>
    {
        // TODO probably want to pass the remaining timeout to flush control.
        if (!_messageWriteLock.Wait(timeout))
            throw new TimeoutException("The operation has timed out.");

        _flushControl.InitializeAsBlocking(timeout);
        try
        {
            UnsynchronizedWriteMessage(_defaultMessageWriter, message).GetAwaiter().GetResult();
            if (_flushControl.WriterCompleted)
                MoveToComplete();
            else
                _defaultMessageWriter.FlushAsync(observeFlushThreshold: false).GetAwaiter().GetResult();
        }
        catch (Exception ex) when (ex is not TimeoutException)
        {
            MoveToComplete(ex);
            throw;
        }
        finally
        {
            if (!IsCompleted)
            {
                _defaultMessageWriter.Reset();
                _flushControl.Reset();
            }
            _messageWriteLock.Release();
        }
    }

    // TODO Complete all with exception
    void MoveToComplete(Exception? exception = null)
    {
        var shouldFlushPipeline = false;
        lock (SyncObj)
        {
            if (_state is PgProtocolState.Completed)
                return;
            _state = PgProtocolState.Completed;
            _completingException = exception;
            shouldFlushPipeline = true;
        }

        //if (shouldFlushPipeline)
    }

    static ValueTask UnsynchronizedWriteMessage<TWriter, T>(MessageWriter<TWriter> writer, T message, CancellationToken cancellationToken = default)
        where TWriter : IBufferWriter<byte> where T : IFrontendMessage<PgV3FrontendHeader>
    {
        if (message.TryPrecomputeHeader(out var header))
        {
            writer.Writer.Ensure(header.Length);
            header.Write(ref writer.Writer);
            message.Write(ref writer.Writer);
            writer.Writer.Commit();
        }
        else if (message is IStreamingFrontendMessage<PgV3FrontendHeader> streamingMessage)
        {
            var result =  streamingMessage.WriteWithHeaderAsync(writer, cancellationToken);
            return result.IsCompletedSuccessfully ? new ValueTask() : new ValueTask(result.AsTask());
        }
        else
        {
            WriteBufferedMessage(message, new HeaderBufferWriter<PgV3FrontendHeader>(), writer, header);
        }

        return new ValueTask();

        static void WriteBufferedMessage(T message, HeaderBufferWriter<PgV3FrontendHeader> headerBufferWriter, MessageWriter<TWriter> writer, PgV3FrontendHeader header)
        {
            try
            {
                var bufferWriter = new BufferWriter<HeaderBufferWriter<PgV3FrontendHeader>>(headerBufferWriter);
                message.Write(ref bufferWriter);
                bufferWriter.Commit();
                header.Length = (int)bufferWriter.BytesCommitted;
                bufferWriter.Output.SetHeader(header);
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

    public ValueTask<T> ReadMessageAsync<T>(T message, CancellationToken cancellationToken = default) where T : IBackendMessage<PgV3Header> => Reader.ReadAsync(this, message, cancellationToken);
    public ValueTask<T> ReadMessageAsync<T>(CancellationToken cancellationToken = default) where T : struct, IBackendMessage<PgV3Header> => Reader.ReadAsync(this, new T(), cancellationToken);
    public T ReadMessage<T>(T message, TimeSpan timeout = default) where T : IBackendMessage<PgV3Header> => Reader.Read(this, message, timeout);
    public T ReadMessage<T>(TimeSpan timeout = default) where T : struct, IBackendMessage<PgV3Header> => Reader.Read(this, new T(), timeout);
    static class Reader
    {
        // As MessageReader is a ref struct we need a small method to create it and pass a reference for the async versions.
        static ReadStatus ReadCore<TMessage>(ref TMessage message, in ReadOnlySequence<byte> sequence, ref MessageReader<PgV3Header>.ResumptionData resumptionData, ref long consumed) where TMessage: IBackendMessage<PgV3Header>
        {
            scoped MessageReader<PgV3Header> reader;
            if (resumptionData.IsDefault)
            {
                reader = MessageReader<PgV3Header>.Create(sequence);
                if (consumed != 0)
                    reader.Advance(consumed);
            }
            else if (consumed == 0)
                reader = MessageReader<PgV3Header>.Resume(sequence, resumptionData);
            else
                reader = MessageReader<PgV3Header>.Create(sequence, resumptionData, consumed);

            var status = message.Read(ref reader);
            consumed = reader.Consumed;
            if (status != ReadStatus.Done)
                resumptionData = reader.GetResumptionData();

            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static int ComputeMinimumSize(long consumed, in MessageReader<PgV3Header>.ResumptionData resumptionData, int maximumMessageChunk)
        {
            uint minimumSize = PgV3Header.ByteCount;
            uint remainingMessage;
            // If we're in a message but it's consumed we assume the reader wants to read the next header.
            // Otherwise we'll return either remainingMessage or maximumMessageChunk, whichever is smaller.
            if (!resumptionData.IsDefault && (remainingMessage = resumptionData.Header.Length - resumptionData.MessageIndex) > 0)
                minimumSize = remainingMessage < maximumMessageChunk ? remainingMessage : Unsafe.As<int, uint>(ref maximumMessageChunk);

            var result = consumed + minimumSize;
            if (result > int.MaxValue)
                ThrowOverflowException();

            return (int)result;

            static void ThrowOverflowException() => throw new OverflowException("Buffers cannot be larger than int.MaxValue, return ReadStatus.ConsumeData to free data while processing.");
        }

        static Exception CreateUnexpectedError<T>(ReadOnlySequence<byte> buffer, scoped in MessageReader<PgV3Header>.ResumptionData resumptionData, long consumed, Exception? readerException = null)
        {
            // Try to read error response.
            Exception exception;
            if (readerException is null && resumptionData.IsDefault == false && resumptionData.Header.Code == BackendCode.ErrorResponse)
            {
                var errorResponse = new ErrorResponse();
                Debug.Assert(resumptionData.MessageIndex <= int.MaxValue);
                consumed -= resumptionData.MessageIndex;
                // Let it start clean, as if it has to MoveNext for the first time.
                MessageReader<PgV3Header>.ResumptionData emptyResumptionData = default;
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

        static ValueTask HandleAsyncResponse(in ReadOnlySequence<byte> buffer, scoped ref MessageReader<PgV3Header>.ResumptionData resumptionData, ref long consumed)
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
            var reader = consumed == 0 ? MessageReader<PgV3Header>.Resume(buffer, resumptionData) : MessageReader<PgV3Header>.Create(buffer, resumptionData, consumed);

            consumed = (int)reader.Consumed;
            throw new NotImplementedException();
        }

        public static T Read<T>(PgV3Protocol protocol, T message, TimeSpan timeout = default) where T : IBackendMessage<PgV3Header>
        {
            ReadStatus status;
            MessageReader<PgV3Header>.ResumptionData resumptionData = default;
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
                        protocol.MoveToComplete(exception);
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
        public static async ValueTask<T> ReadAsync<T>(PgV3Protocol protocol, T message, CancellationToken cancellationToken = default) where T : IBackendMessage<PgV3Header>
        {
            ReadStatus status;
            MessageReader<PgV3Header>.ResumptionData resumptionData = default;
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
                        protocol.MoveToComplete(exception);
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

    async ValueTask WriteInternalAsync<T>(T message, CancellationToken cancellationToken = default) where T : IFrontendMessage<PgV3FrontendHeader>
    {
        _flushControl.Initialize();
        try
        {
            await UnsynchronizedWriteMessage(_defaultMessageWriter, message, cancellationToken).ConfigureAwait(false);
            if (_flushControl.WriterCompleted)
                MoveToComplete();
            else
                await _defaultMessageWriter.FlushAsync(observeFlushThreshold: false, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not TimeoutException && (ex is not OperationCanceledException || ex is OperationCanceledException oce && oce.CancellationToken != cancellationToken))
        {
            MoveToComplete(ex);
            throw;
        }
        finally
        {
            if (!IsCompleted)
            {
                _defaultMessageWriter.Reset();
                _flushControl.Reset();
            }
        }
    }

    static async ValueTask<PgV3Protocol> StartAsyncCore(PgV3Protocol conn, PgOptions options)
    {
        try
        {
            await conn.WriteInternalAsync(new StartupRequest(options));
            var msg = await conn.ReadMessageAsync(new AuthenticationRequest()).ConfigureAwait(false);
            switch (msg.AuthenticationType)
            {
                case AuthenticationType.Ok:
                    await conn.ReadMessageAsync<StartupResponse>().ConfigureAwait(false);
                    break;
                case AuthenticationType.MD5Password:
                    if (options.Password is null)
                        throw new InvalidOperationException("No password given, connection expects password.");
                    await conn.WriteInternalAsync(new PasswordMessage(options.Username, options.Password, msg.MD5Salt));
                    var expectOk = await conn.ReadMessageAsync(new AuthenticationRequest()).ConfigureAwait(false);
                    if (expectOk.AuthenticationType != AuthenticationType.Ok)
                        throw new Exception("Unexpected authentication response");
                    await conn.ReadMessageAsync<StartupResponse>().ConfigureAwait(false);
                    break;
                case AuthenticationType.CleartextPassword:
                default:
                    throw new Exception();
            }

            conn._state = PgProtocolState.Ready;
            return conn;
        }
        catch (Exception)
        {
            conn.Dispose();
            throw;
        }
    }

    public static ValueTask<PgV3Protocol> StartAsync(PipeWriter writer, PipeReader reader, PgOptions options, PgV3ProtocolOptions? pipeOptions = null)
    {
        var conn = new PgV3Protocol(writer, reader, pipeOptions);
        return StartAsyncCore(conn, options);
    }

    public static ValueTask<PgV3Protocol> StartAsync(IPipeWriterSyncSupport writer, IPipeReaderSyncSupport reader, PgOptions options, PgV3ProtocolOptions? pipeOptions = null)
    {
        var conn = new PgV3Protocol(writer, reader, pipeOptions);
        return StartAsyncCore(conn, options);
    }

    public static PgV3Protocol Start(IPipeWriterSyncSupport writer, IPipeReaderSyncSupport reader, PgOptions options, PgV3ProtocolOptions? pipeOptions = null)
    {
        try
        {
            var conn = new PgV3Protocol(writer, reader, pipeOptions);
            conn.WriteMessage(new StartupRequest(options));
            var msg = conn.ReadMessage(new AuthenticationRequest());
            switch (msg.AuthenticationType)
            {
                case AuthenticationType.Ok:
                    conn.ReadMessage<StartupResponse>();
                    break;
                case AuthenticationType.MD5Password:
                    if (options.Password is null)
                        throw new InvalidOperationException("No password given, connection expects password.");
                    conn.WriteMessage(new PasswordMessage(options.Username, options.Password, msg.MD5Salt));
                    var expectOk = conn.ReadMessage(new AuthenticationRequest());
                    if (expectOk.AuthenticationType != AuthenticationType.Ok)
                        throw new Exception("Unexpected authentication response");
                    conn.ReadMessage<StartupResponse>();
                    break;
                case AuthenticationType.CleartextPassword:
                default:
                    throw new Exception();
            }

            // Safe to change outside lock, we haven't exposed the instance yet.
            conn._state = PgProtocolState.Ready;
            return conn;
        }
        catch (Exception ex)
        {
            writer.PipeWriter.Complete(ex);
            reader.PipeReader.Complete(ex);
            throw;
        }
    }

    void EnqueueUnsynchronized(PgV3OperationSource source, CancellationToken cancellationToken)
    {
        if (source.IsExclusiveUse)
            _pendingExclusiveUses++;

        if (!source.IsPooled)
            source.WithCancellationToken(cancellationToken);

        source.BeginWrites(_messageWriteLock, cancellationToken);
        _operations.Enqueue(source);
    }

    public override bool TryStartOperation(OperationSlot slot, OperationBehavior behavior = OperationBehavior.None, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        var source = ThrowIfInvalidSlot(slot, allowUnbound: true, allowActivated: false);

        if (!source.IsUnbound)
            throw new ArgumentException("Cannot start an operation with a slot that is already bound.", nameof(slot));

        lock (SyncObj)
        {
            int count;
            if (_state is not PgProtocolState.Ready || ((count = _operations.Count) > 0 && behavior.HasImmediateOnly()))
                return false;

            source.Bind(this, behavior.HasExclusiveUse(), cancellationToken);
            EnqueueUnsynchronized(source, cancellationToken);
            if (count == 0)
                source.Activate();
        }

        return true;
    }

    public override bool TryStartOperation([NotNullWhen(true)]out OperationSlot? slot, OperationBehavior behavior = OperationBehavior.None, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        PgV3OperationSource source;
        lock (SyncObj)
        {
            int count;
            if (_state is not PgProtocolState.Ready || ((count = _operations.Count) > 0 && behavior.HasImmediateOnly()))
            {
                slot = null;
                return false;
            }

            var exclusiveUse = behavior.HasExclusiveUse();
            if (count == 0)
            {
                source = exclusiveUse ? _exclusiveOperationSourceSingleton : _operationSourceSingleton;
                source.Reset();
            }
            else
                source = new PgV3OperationSource(this, exclusiveUse, pooled: false);

            EnqueueUnsynchronized(source, cancellationToken);
        }

        slot = source;
        return true;
    }

    void CompleteOperation(PgV3OperationSource operationSource, Exception? exception)
    {
        PgV3OperationSource currentSource;
        bool hasNext = false;
        lock (SyncObj)
        {
            // TODO we may be able to build a linked list instead of needing a queue.
            if (_operations.TryPeek(out currentSource!) && ReferenceEquals(currentSource, operationSource))
            {
                _operations.Dequeue();
                currentSource.EndWrites(_messageWriteLock);
                if (currentSource.IsExclusiveUse)
                    _pendingExclusiveUses--;
                while ((hasNext = _operations.TryPeek(out currentSource!)) && currentSource.IsCompleted && _operations.TryDequeue(out _))
                {}
            }
        }

        if (exception is not null)
        {
            // As long as _completingException is null we don't really care that an operation ended on an exception, we can log it here though.
        }

        // Activate the next uncompleted one, outside the lock.
        if (hasNext)
        {
            if (_completingException is not null)
            {
                // Just drain by pushing the last exception down
                while (_operations.TryDequeue(out currentSource!) && !currentSource.IsCompleted)
                {
                    currentSource.TryComplete(new Exception("The connection was previously broken because of the following exception", _completingException));
                }
            }
            else
                currentSource.Activate();
        }
    }

    public override async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        PgV3OperationSource? source;
        lock (SyncObj)
        {
            if (_state is PgProtocolState.Draining or PgProtocolState.Completed)
                return;

            _state = PgProtocolState.Draining;
            source = _operations.Count > 0 ? new PgV3OperationSource(this, exclusiveUse: true, pooled: false) : null;
            if (source is not null)
                // Don't enqueue with cancellationtoken, we wait out of band later on.
                // this is to make sure that once we drain we don't stop waiting until we're empty and completed.
                EnqueueUnsynchronized(source, CancellationToken.None);
        }

        Exception? opException = null;
        try
        {
            if (source is not null)
                await source.Task.AsTask().WaitAsync(cancellationToken);
        }
        catch(Exception ex)
        {
            opException = ex;
        }
        finally
        {
            MoveToComplete(opException);
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;
        _disposed = true;
        _state = PgProtocolState.Completed;
        _pipeWriter.Complete();
        _reader.Complete();
        _messageWriteLock.Dispose();
        _flushControl.Dispose();
    }

    public override PgProtocolState State => _state;

    // No locks as it doesn't have to be accurate.
    public override bool PendingExclusiveUse => _pendingExclusiveUses != 0;
    public override int Pending => _operations.Count;

    public static OperationSource CreateUnboundOperationSource(CancellationToken cancellationToken)
        => new PgV3OperationSource(null, false, false);

    sealed class PgV3OperationSource : OperationSource
    {
        public PgV3OperationSource(PgV3Protocol? protocol, bool exclusiveUse, bool pooled)
            : base(protocol, asyncContinuations: true, pooled)
        {
            _exclusiveUse = exclusiveUse;
        }

        // Will be initialized during TakeWriteLock.
        volatile Task? _writeSlot;
        bool _exclusiveUse;

        PgV3Protocol? GetProtocol()
        {
            var protocol = Protocol;
            return protocol is null ? null : Unsafe.As<PgProtocol, PgV3Protocol>(ref protocol);
        }

        public object? InstanceToken => GetProtocol();
        public Task WriteSlot => _writeSlot!;
        public bool IsExclusiveUse
        {
            get => _exclusiveUse;
            private set
            {
                if (!IsUnbound)
                    throw new InvalidOperationException("Cannot change after binding");

                _exclusiveUse = value;
            }
        }

        public bool IsUnbound => Protocol is null;

        public void Bind(PgV3Protocol protocol, bool exclusiveUse, CancellationToken cancellationToken)
        {
            IsExclusiveUse = exclusiveUse;
            WithCancellationToken(cancellationToken);
            BindCore(protocol);
        }

        public PgV3OperationSource WithCancellationToken(CancellationToken cancellationToken)
        {
            AddCancellation(cancellationToken);
            return this;
        }

        public void Activate() => ActivateCore();

        protected override void CompleteCore(PgProtocol protocol, Exception? exception)
            => Unsafe.As<PgProtocol, PgV3Protocol>(ref protocol).CompleteOperation(this, exception);

        protected override void ResetCore()
        {
            _writeSlot = null;
        }

        // TODO ideally we'd take the lock opportunistically, not sure how though as write congruence with queue position is critical.
        public void BeginWrites(SemaphoreSlim writelock, CancellationToken cancellationToken)
        {
            Debug.Assert(_writeSlot is null, "WriteSlot was already set, instance was not properly finished?");
            if (!writelock.Wait(0))
                _writeSlot = writelock.WaitAsync(cancellationToken);
            else
                _writeSlot = System.Threading.Tasks.Task.CompletedTask;
        }

        public bool EndWrites(SemaphoreSlim writelock)
        {
            if (_writeSlot is null || !_writeSlot.IsCompleted || (IsExclusiveUse && !IsCompleted))
                return false;

            // Null out the completed task before we release.
            _writeSlot = null!;
            writelock.Release();
            return true;
        }
    }
}

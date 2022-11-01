using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Pipelines.Buffers;
using Npgsql.Pipelines.Protocol.PgV3.Commands;
using FlushResult = Npgsql.Pipelines.Buffers.FlushResult;

namespace Npgsql.Pipelines.Protocol.PgV3;

record PgV3ProtocolOptions
{
    public TimeSpan ReadTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan WriteTimeout { get; init;  } = TimeSpan.FromSeconds(10);
    // public int ReaderSegmentSize { get; init; } = 8192;
    // public int WriterSegmentSize { get; init; } = 8192;
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

    volatile PgProtocolState _state = PgProtocolState.Created;
    volatile bool _disposed;
    volatile Exception? _completingException;
    volatile int _pendingExclusiveUses;

    PgV3Protocol(IPipeWriterSyncSupport writer, IPipeReaderSyncSupport reader, PgV3ProtocolOptions? protocolOptions = null)
    {
        _protocolOptions = protocolOptions ?? DefaultPipeOptions;
        _pipeWriter = writer.PipeWriter;
        _flushControl = new ResettableFlushControl(writer, _protocolOptions.WriteTimeout, Math.Max(MessageWriter.DefaultAdvisoryFlushThreshold , _protocolOptions.FlushThreshold));
        _defaultMessageWriter = new MessageWriter<IPipeWriterSyncSupport>(writer, _flushControl);
        _reader = new SimplePipeReader(reader, _protocolOptions.ReadTimeout);
        _operations = new Queue<PgV3OperationSource>();
        _operationSourceSingleton = new PgV3OperationSource(this, exclusiveUse: false, pooled: true);
        _exclusiveOperationSourceSingleton = new PgV3OperationSource(this, exclusiveUse: true, pooled: true);
        _commandReaderSingleton = new CommandReader();
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

    public override ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        return FlushAsyncCore(null, cancellationToken);
    }

    public override ValueTask FlushAsync(OperationSlot op, CancellationToken cancellationToken = default)
    {
        return FlushAsyncCore(op, cancellationToken);
    }

    async ValueTask FlushAsyncCore(OperationSlot? op = null, CancellationToken cancellationToken = default)
    {
        if (_pipeWriter.UnflushedBytes == 0)
            return;

        // TODO actually check if the passed slot is the head.
        if (op is null && !_messageWriteLock.Wait(0))
            await _messageWriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        _flushControl.Initialize();
        try
        {
            await _flushControl.FlushAsync(observeFlushThreshold: false, cancellationToken: cancellationToken).ConfigureAwait(false);
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

            if (op is null && !_disposed)
                _messageWriteLock.Release();
        }
    }

    // TODO maybe add some ownership transfer logic here, allocating new instances if the singleton isn't back yet.
    public override CommandReader GetCommandReader() => _commandReaderSingleton;

    public IOCompletionPair WriteMessageAsync<T>(OperationSlot slot, T message, bool flushHint = true, CancellationToken cancellationToken = default) where T : IFrontendMessage
        => WriteMessageBatchAsync(slot, (batchWriter, message, cancellationToken) => batchWriter.WriteMessageAsync(message, cancellationToken), message, flushHint, cancellationToken);

    public readonly struct BatchWriter
    {
        readonly PgV3Protocol _protocol;
        internal BatchWriter(PgV3Protocol protocol) => _protocol = protocol;

        public ValueTask WriteMessageAsync<T>(T message, CancellationToken cancellationToken = default) where T : IFrontendMessage
            => WriteMessageUnsynchronized(_protocol._defaultMessageWriter, message, cancellationToken);

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
            if (!source.WriteSlot.IsCompletedSuccessfully)
                await source.WriteSlot.ConfigureAwait(false);

            instance._flushControl.Initialize();
            try
            {
                var writeTask = batchWriter.Invoke(new BatchWriter(instance), state, cancellationToken);
                if (!writeTask.IsCompletedSuccessfully)
                    await writeTask.ConfigureAwait(false);
                if (instance._flushControl.WriterCompleted)
                    instance.MoveToComplete();
                else if (flushHint && instance._flushControl.UnflushedBytes > 0)
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

                // TODO not sure I'm happy with ending writes *automatically* after one write.
                if (!instance._disposed && !source.IsExclusiveUse)
                {
                    var result = source.EndWrites(instance._messageWriteLock);
                    Debug.Assert(result, "Could not end write slot.");
                }
            }
        }
    }

    // TODO this is not up-to-date with the async implementation.
    public void WriteMessage<T>(T message, TimeSpan timeout = default) where T : IFrontendMessage
    {
        // TODO probably want to pass the remaining timeout to flush control.
        if (!_messageWriteLock.Wait(timeout))
            throw new TimeoutException("The operation has timed out.");

        _flushControl.InitializeAsBlocking(timeout);
        try
        {
            WriteMessageUnsynchronized(_defaultMessageWriter, message).GetAwaiter().GetResult();
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
        var shouldDrain = false;
        lock (SyncObj)
        {
            if (_state is PgProtocolState.Completed)
                return;
            _state = PgProtocolState.Completed;
            _completingException = exception;
            shouldDrain = true;
        }

        //if (shouldDrain)
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ValueTask WriteMessageUnsynchronized<TWriter, T>(MessageWriter<TWriter> writer, T message, CancellationToken cancellationToken = default)
        where TWriter : IBufferWriter<byte> where T : IFrontendMessage
    {
        if (message.CanWrite)
        {
            message.Write(ref writer.Writer);
            writer.Writer.Commit();
            return new ValueTask();
        }

        if (message is IStreamingFrontendMessage streamingMessage)
        {
            var result =  streamingMessage.WriteAsync(writer, cancellationToken);
            return result.IsCompletedSuccessfully ? new ValueTask() : new ValueTask(result.AsTask());
        }

        ThrowCannotWrite();
        return new ValueTask();

        void ThrowCannotWrite() => throw new InvalidOperationException("Either CanWrite should return true or IStreamingFrontendMessage should be implemented to write this message.");
    }

    public async ValueTask WaitForDataAsync(int minimumSize, CancellationToken cancellationToken = default)
    {
        await _reader.ReadAtLeastAsync(minimumSize, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<T> ReadMessageAsync<T>(T message, CancellationToken cancellationToken = default) where T : IBackendMessage<PgV3Header> => ProtocolReader.ReadAsync(this, message, cancellationToken);
    public ValueTask<T> ReadMessageAsync<T>(CancellationToken cancellationToken = default) where T : struct, IBackendMessage<PgV3Header> => ProtocolReader.ReadAsync(this, new T(), cancellationToken);
    public T ReadMessage<T>(T message, TimeSpan timeout = default) where T : IBackendMessage<PgV3Header> => ProtocolReader.Read(this, message, timeout);
    public T ReadMessage<T>(TimeSpan timeout = default) where T : struct, IBackendMessage<PgV3Header> => ProtocolReader.Read(this, new T(), timeout);
    static class ProtocolReader
    {
        // As MessageReader is a ref struct we need a small method to create it and pass a reference for the async versions.
        static ReadStatus ReadCore<TMessage>(ref TMessage message, in ReadOnlySequence<byte> sequence, ref MessageReader<PgV3Header>.ResumptionData resumptionData, ref long consumed, bool resuming) where TMessage: IBackendMessage<PgV3Header>
        {
            scoped MessageReader<PgV3Header> reader;
            if (!resuming)
                reader = MessageReader<PgV3Header>.Create(sequence);
            else if (consumed == 0)
                reader = MessageReader<PgV3Header>.Resume(sequence, resumptionData);
            else
                reader = MessageReader<PgV3Header>.Recreate(sequence, resumptionData, consumed);

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
            if (readerException is null && !resumptionData.IsDefault && resumptionData.Header.Code == BackendCode.ErrorResponse)
            {
                var errorResponse = new ErrorResponse();
                Debug.Assert(resumptionData.MessageIndex <= int.MaxValue);
                consumed -= resumptionData.MessageIndex;
                // Let it start clean, as if it has to MoveNext for the first time.
                MessageReader<PgV3Header>.ResumptionData emptyResumptionData = default;
                var errorResponseStatus = ReadCore(ref errorResponse, buffer, ref emptyResumptionData, ref consumed, false);
                if (errorResponseStatus != ReadStatus.Done)
                    exception = new Exception($"Unexpected error on message: {typeof(T).FullName}, could not read full error response, terminated connection.");
                else
                    exception = new Exception($"Unexpected error on message: {typeof(T).FullName}, error message: {errorResponse.ErrorOrNoticeMessage.Message}.");
            }
            else
            {
                exception = new Exception($"Protocol desync on message: {typeof(T).FullName}, expected different response{(resumptionData.IsDefault ? "" : ", actual code: " + resumptionData.Header.Code)}.", readerException);
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
            var reader = consumed == 0 ? MessageReader<PgV3Header>.Resume(buffer, resumptionData) : MessageReader<PgV3Header>.Recreate(buffer, resumptionData, consumed);

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
            var resumed = false;
            do
            {
                var buffer =protocol._reader.ReadAtLeast(ComputeMinimumSize(consumed, resumptionData, protocol._protocolOptions.MaximumMessageChunkSize), readTimeout);

                try
                {
                    status = ReadCore(ref message, buffer, ref resumptionData, ref consumed, resumed);
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

                resumed = true;
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
            var resuming = false;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                var buffer = await protocol._reader.ReadAtLeastAsync(ComputeMinimumSize(consumed, resumptionData, protocol._protocolOptions.MaximumMessageChunkSize), cancellationToken).ConfigureAwait(false);

                try
                {
                    status = ReadCore(ref message, buffer, ref resumptionData, ref consumed, resuming);
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
                        await HandleAsyncResponse(buffer, ref resumptionData, ref consumed).ConfigureAwait(false);
                        break;
                }

                resuming = true;
            } while (status != ReadStatus.Done);

            return message;
        }
    }

    async ValueTask WriteInternalAsync<T>(T message, CancellationToken cancellationToken = default) where T : IFrontendMessage
    {
        _flushControl.Initialize();
        try
        {
            await WriteMessageUnsynchronized(_defaultMessageWriter, message, cancellationToken).ConfigureAwait(false);
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
            await conn.WriteInternalAsync(new StartupRequest(options)).ConfigureAwait(false);
            var msg = await conn.ReadMessageAsync(new AuthenticationRequest()).ConfigureAwait(false);
            switch (msg.AuthenticationType)
            {
                case AuthenticationType.Ok:
                    await conn.ReadMessageAsync<StartupResponse>().ConfigureAwait(false);
                    break;
                case AuthenticationType.MD5Password:
                    if (options.Password is null)
                        throw new InvalidOperationException("No password given, connection expects password.");
                    await conn.WriteInternalAsync(new PasswordMessage(options.Username, options.Password, msg.MD5Salt)).ConfigureAwait(false);
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
        // ThrowIfDisposed();
        // cancellationToken.ThrowIfCancellationRequested();
        // var source = ThrowIfInvalidSlot(slot, allowUnbound: true, allowActivated: false);

        if (slot is not PgV3OperationSource source)
            throw new InvalidOperationException();

        int count;
        lock (SyncObj)
        {
            if (_state is not PgProtocolState.Ready || ((count = _operations.Count) > 0 && behavior.HasImmediateOnly()))
                return false;

            if (behavior.HasExclusiveUse())
                source.IsExclusiveUse = true;
            source.Bind(this);
            EnqueueUnsynchronized(source, cancellationToken);
        }

        if (count == 0)
            source.Activate();

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
                var result = currentSource.EndWrites(_messageWriteLock);
                Debug.Assert(result, "Could not end write slot.");
                if (currentSource.IsExclusiveUse)
                    _pendingExclusiveUses--;
                while ((hasNext = _operations.TryPeek(out currentSource!)) && currentSource.IsCompleted && _operations.TryDequeue(out _))
                {}
            }
        }

        if (exception is not null && _completingException is null)
        {
            _completingException = exception;
            // TODO Mhmmm
            var _ = Task.Run(() => CompleteAsync(exception));
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

    public override async Task CompleteAsync(Exception? exception = null, CancellationToken cancellationToken = default)
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

        Exception? opException = exception;
        try
        {
            if (source is not null)
                await source.Task.AsTask().WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch(Exception ex)
        {
            if (opException is null)
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
        // Will be initialized during TakeWriteLock.
        volatile Task? _writeSlot;

        bool _exclusiveUse;

        public PgV3OperationSource(PgV3Protocol? protocol, bool exclusiveUse, bool pooled)
            : base(protocol, asyncContinuations: true, pooled)
        {
            _exclusiveUse = exclusiveUse;
        }

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
            internal set
            {
                if (!IsUnbound)
                    ThrowAlreadyBound();

                _exclusiveUse = value;

                static void ThrowAlreadyBound() => throw new InvalidOperationException("Cannot change after binding");
            }
        }

        public bool IsUnbound => !IsCompleted && Protocol is null && !IsPooled;

        public void Bind(PgV3Protocol protocol) => BindCore(protocol);

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
            if (_writeSlot is null || !_writeSlot.IsCompleted)
                return false;

            // Null out the completed task before we release.
            _writeSlot = null!;
            writelock.Release();
            return true;
        }
    }

    long _statementCounter = 0;
    Dictionary<Guid, string> _trackedStatements { get; } = new();

    /// <summary>
    /// 
    /// </summary>
    /// <param name="statement"></param>
    /// <param name="name"></param>
    /// <returns>True if added, false if it was already added.</returns>
    /// <exception cref="NotImplementedException"></exception>
    public bool GetOrAddStatementName(in Statement statement, out string name)
    {
        lock (_trackedStatements)
        {
            if (_trackedStatements.TryGetValue(statement.Id, out name!))
                return false;

            name = statement.Kind switch
            {
                PreparationKind.Auto => $"A${_statementCounter++}",
                PreparationKind.Command => $"C${_statementCounter++}",
                PreparationKind.Global => $"G${_statementCounter++}",
                _ => throw new ArgumentOutOfRangeException()
            };

            _trackedStatements[statement.Id] = name;
            return true;
        }
    }
}

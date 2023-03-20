using System;
using System.Buffers;
using System.Collections;
using System.Collections.Specialized;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg;
using Slon.Pg.Descriptors;
using Slon.Pg.Converters;
using Slon.Pg.Types;
using Slon.Protocol;
using Slon.Protocol.Pg;
using Slon.Protocol.PgV3; // TODO

namespace Slon;

enum ReaderState
{
    Uninitialized = 0,
    Active,
    Completed,
    Exhausted,
    Closed,
}

// TODO should close the WritableParameters if any (this will also dispose the underlying pooledmemory).

// Implementation
public sealed partial class SlonDataReader
{
    static ObjectPool<SlonDataReader>? _sharedPool;
    static ObjectPool<SlonDataReader> SharedPool =>
        _sharedPool ??= new(pool =>
        {
            var returnAction = pool.Return;
            return () => new SlonDataReader(returnAction);
        });

    readonly Action<SlonDataReader>? _returnAction;

    // Will be set during initialization.
    SlonDataSource _dataSource = null!;
    PgV3CommandReader _commandReader = null!;
    CommandContextBatch<CommandExecution>.Enumerator _commandEnumerator;

    ReaderState _state;
    ulong? _recordsAffected;

    // This is not a pooled method as it quickly uses up all the pooled instances during pipelining, meanign we only pay for the overhead of pooling.
    // Improvement of this code (and removing the alloc) is ideally dependent on something like: https://github.com/dotnet/runtime/issues/78064
    internal static async ValueTask<SlonDataReader> Create(bool async, SlonDataSource dataSource, ValueTask<CommandContextBatch<CommandExecution>> batch)
    {
        // If the enumerator task fails there is not much we can cleanup (or should have to).
        CommandContextBatch<CommandExecution>.Enumerator enumerator = (await batch.ConfigureAwait(false)).GetEnumerator();
        var result = enumerator.MoveNext();
        DebugShim.Assert(result); // We should always get one.

        PgV3CommandReader? commandReader = null;
        Operation? operation = null;
        try
        {
            operation = await enumerator.Current.GetOperation().ConfigureAwait(false);
            commandReader = operation.GetValueOrDefault().Protocol.GetCommandReader();
            // Immediately initialize the first command, we're supposed to be positioned there at the start.
            await commandReader.InitializeAsync(enumerator.Current).ConfigureAwait(false);
        }
        catch(Exception ex)
        {
            // If we have a write side failure we have not set any operation, yet we're not completed either, get our read op directly.
            // As we did not reach commandReader.InitializeAsync we complete with an exception, the protocol is in an indeterminate state.
            if (operation is null)
                (await enumerator.Current.ReadSlot.Task.ConfigureAwait(false)).Complete(ex);

            await ConsumeBatch(async, enumerator, commandReader).ConfigureAwait(false);
            throw;
        }

        return SharedPool.Rent().Initialize(dataSource, commandReader, enumerator, operation.Value);
    }

    static ValueTask ConsumeBatch(bool async, CommandContextBatch<CommandExecution>.Enumerator enumerator, PgV3CommandReader? commandReader = null)
    {
        if ((commandReader is null || commandReader.State is CommandReaderState.Completed or CommandReaderState.UnrecoverablyCompleted) && !enumerator.MoveNext())
            return new ValueTask();

        return Core();

        async ValueTask Core()
        {
            // TODO figure out what we *actually* would have to do here for batches.
            try
            {
                if (commandReader is not null)
                    await commandReader.CloseAsync();

                var result = enumerator.MoveNext();
                DebugShim.Assert(!result);
            }
            catch
            {
                // We swallow any remaining exceptions (maybe we want to aggregate though).
            }
        }
    }

    SlonDataReader Initialize(SlonDataSource dataSource, PgV3CommandReader reader, CommandContextBatch<CommandExecution>.Enumerator enumerator, Operation firstOp)
    {
        _state = ReaderState.Active;
        _dataSource = dataSource;
        _commandReader = reader;
        _commandEnumerator = enumerator;
        SyncStates();
        return this;
    }

    void SyncStates()
    {
        DebugShim.Assert(_commandReader is not null);
        switch (_commandReader.State)
        {
            case CommandReaderState.Initialized:
                _state = ReaderState.Active;
                break;
            case CommandReaderState.Completed:
                HandleCompleted();
                break;
            case CommandReaderState.UnrecoverablyCompleted:
                if (_state is not ReaderState.Uninitialized or ReaderState.Closed)
                    _state = ReaderState.Closed;
                break;
            case CommandReaderState.Active:
                break;
            case CommandReaderState.None:
            default:
                ThrowArgumentOutOfRange(_commandReader.State);
                break;
        }

        void ThrowArgumentOutOfRange(CommandReaderState state)
            => throw new ArgumentOutOfRangeException(nameof(_commandReader.State), state, "Unexpected case.");
        void HandleCompleted()
        {
            // Store this before we move on.
            if (_state is ReaderState.Active)
            {
                _state = ReaderState.Completed;
                if (!_recordsAffected.HasValue)
                    _recordsAffected = 0;
                _recordsAffected += _commandReader.RowsAffected;
            }
        }
    }

    // If this changes make sure to modify any of the inlined _state checks in Read/ReadAsync etc.
    Exception? ThrowIfClosedOrDisposed(ReaderState? readerState = null, bool returnException = false)
    {
        DebugShim.Assert(_commandReader is not null);
        var exception = (readerState ?? _state) switch
        {
            ReaderState.Uninitialized => new ObjectDisposedException(nameof(SlonDataReader)),
            ReaderState.Closed => new InvalidOperationException("Reader is closed."),
            _ => null
        };

        if (exception is null)
            return null;

        return returnException ? exception : throw exception;
    }

    // Any changes to this method should be reflected in Create.
    async Task<bool> NextResultAsyncCore(CancellationToken cancellationToken = default)
    {
        if (_state is ReaderState.Exhausted || !_commandEnumerator.MoveNext())
        {
            _state = ReaderState.Exhausted;
            return false;
        }

        try
        {
            await _commandReader.InitializeAsync(_commandEnumerator.Current, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            SyncStates();
        }
    }

    async ValueTask CloseCore(bool async, ReaderState? state = null)
    {
        if ((state ?? _state) is ReaderState.Closed or ReaderState.Uninitialized)
            return;

        try
        {
            await ConsumeBatch(async, _commandEnumerator, _commandReader);
        }
        finally
        {
            if (state is null)
                _state = ReaderState.Closed;
        }
    }

#if !NETSTANDARD2_0
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
    async ValueTask DisposeCore(bool async)
    {
        var state = _state;
        if (state is ReaderState.Uninitialized)
            return;
        _state = ReaderState.Uninitialized;

        ExceptionDispatchInfo? edi = null;
        try
        {
            await CloseCore(async, state).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            edi = ExceptionDispatchInfo.Capture(e);
        }
        _commandEnumerator.Dispose();
        _commandEnumerator = default;
        var commandReader = _commandReader;
        _commandReader = null!;
        commandReader.Reset();
        _returnAction?.Invoke(this);
        edi?.Throw();
    }
}

// Public surface & ADO.NET
public sealed partial class SlonDataReader: DbDataReader
{
    internal SlonDataReader(Action<SlonDataReader>? returnAction = null) => _returnAction = returnAction;

    public override int Depth => 0;
    public override int FieldCount
    {
        get
        {
            ThrowIfClosedOrDisposed();
            return _commandReader.FieldCount;
        }
    }
    public override object this[int ordinal] => throw new NotImplementedException();
    public override object this[string name] => throw new NotImplementedException();

    public override int RecordsAffected
    {
        get
        {
            ThrowIfClosedOrDisposed();
            return !_recordsAffected.HasValue
                ? -1
                : _recordsAffected > int.MaxValue
                    ? throw new OverflowException(
                        $"The number of records affected exceeds int.MaxValue. Use {nameof(Rows)}.")
                    : (int)_recordsAffected;
        }
    }

    public ulong Rows
    {
        get
        {
            ThrowIfClosedOrDisposed();
            return _recordsAffected ?? 0;
        }
    }

    public override bool HasRows
    {
        get
        {
            ThrowIfClosedOrDisposed();
            return _commandReader.HasRows;
        }
    }
    public override bool IsClosed => _state is ReaderState.Closed or ReaderState.Uninitialized;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        if (_state is var state and (ReaderState.Closed or ReaderState.Uninitialized))
            Task.FromException(ThrowIfClosedOrDisposed(state, returnException: true)!);
        return _commandReader.ReadAsync(cancellationToken);
    }

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        if (_state is var state and (ReaderState.Closed or ReaderState.Uninitialized))
            Task.FromException(ThrowIfClosedOrDisposed(state, returnException: true)!);
        return NextResultAsyncCore(cancellationToken);
    }

    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public override T GetFieldValue<T>(int ordinal)
    {
        var reader = new PgReader();
        // TODO store last used converter per column for quick checking.
        var field = new Field();
        var info = _dataSource.GetConverterInfo(typeof(T), field);
        if (typeof(T) == typeof(object) && info.Type != typeof(object))
        {
            return (T)info.GetResolutionAsObject(field).Converter.ReadAsObject(reader)!;
        }

        return info.GetResolution<T>(field).Converter.Read(reader) ?? throw new InvalidOperationException("DbNull returned");
    }

    public override async Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
    {
                var options = new PgConverterOptions()
        {
            TextEncoding = Encoding.UTF8,
            ConverterInfoResolver = null!,
            TypeCatalog = PgTypeCatalog.Default.ToPortableCatalog()
        };
        var dtTz = PgConverterInfo.Create(options, new DateTimeConverterResolver(DataTypeNames.TimestampTz, DataTypeNames.Timestamp), DataTypeNames.TimestampTz);
        var dtoTz = PgConverterInfo.Create(options, new DateTimeOffsetUtcOnlyConverterResolver(DataTypeNames.TimestampTz), DataTypeNames.TimestampTz);
        dtTz.Compose(new ArrayConverterResolver<DateTime>(dtTz), options.GetArrayTypeId(dtTz.PgTypeId!.Value));
        dtoTz.Compose(new ArrayConverterResolver<DateTimeOffset>(dtoTz), options.GetArrayTypeId(dtoTz.PgTypeId!.Value));

        var bitArray = PgConverterInfo.Create(options, new BitArrayBitStringConverter(options), DataTypeNames.Varbit).GetResolution(default(BitArray));
        var bitVector32 = PgConverterInfo.Create(options, new BitVector32BitStringConverter(), DataTypeNames.Varbit).GetResolution(default(BitVector32));
        var booleanBitString = PgConverterInfo.Create(options, new BoolBitStringConverter(), DataTypeNames.Varbit).GetResolution(default(bool));
        var objectBitstring = PgConverterInfo.Create(options, new PolymorphicBitStringConverterResolver(DataTypeNames.Varbit), DataTypeNames.Varbit).GetResolutionAsObject(default);

        var charArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<char>(new(new CharTextConverter(options), default), ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Bpchar.ToArrayName()).GetResolution(default(char));
        var stringArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<string>(new(new StringTextConverter(new ReadOnlyMemoryTextConverter(options), options), default), ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(string));
        var boolArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<bool>(new(new BoolConverter(), default), ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(bool));
        // var shortArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<short>(new(new Int16Converter<short>(), default), ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(short));
        // var intArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<int>(new(new Int32Converter<int>(), default), ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(int));
        // var longArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<long>(new(new Int64Converter<long>(), default), ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(long));
        // var ushortArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<ushort>(new(new Int16Converter<ushort>(), default), ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(ushort));
        // var uintArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<uint>(new(new Int32Converter<uint>(), default), ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(uint));
        // var ulongArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<ulong>(new(new Int64Converter<ulong>(), default), ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(ulong));
        var decimalArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<decimal>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(decimal));
        var doubleArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<double>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(double));
        var floatArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<float>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(float));
        var bigintegerArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<BigInteger>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(BigInteger));
        var guidArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<Guid>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(Guid));
#if !NETSTANDARD2_0
        var dateArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<DateOnly>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(DateOnly));
        var timeArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<TimeOnly>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(TimeOnly));
#endif
        var timespanArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<TimeSpan>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(TimeSpan));
        var dateTimeArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<DateTime>(new(new DateTimeConverter(options, DateTimeKind.Unspecified), default), ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(DateTime));
        var dateTimeOffsetArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<DateTimeOffset>(new(new DateTimeOffsetConverter(options), default), ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(DateTimeOffset));

        var cidrArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<(IPAddress Address, int Subnet)>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default((IPAddress, int)));

        var arraySegmentArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<ArraySegment<byte>>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(ArraySegment<byte>));
        var romArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<ReadOnlyMemory<byte>>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(ReadOnlyMemory<byte>));
        var memoryArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<Memory<byte>>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(Memory<byte>));
        var bitvectorArrayConverter = PgConverterInfo.Create(options, new ArrayConverter<BitVector32>(default, ArrayPool<(ValueSize, object?)>.Shared), DataTypeNames.Unknown).GetResolution(default(BitVector32));

        // total: 32 instantiations

        // Missing value type instantiations (13) :
        // - range
        // - box
        // - circle
        // - polygon
        // - point
        // - path
        // - line segment
        // - line
        // - pg decimal
        // - pg interval
        // - inet
        // - tid
        // - lsn

        var reader = new PgReader();
        // TODO store last used converter per column for quick checking.
        var field = new Field();
        var info = _dataSource.GetConverterInfo(typeof(T), field);
        if (typeof(T) == typeof(object) && info.Type != typeof(object))
        {
            return (T)(await info.GetResolutionAsObject(field).Converter.ReadAsObjectAsync(reader, cancellationToken) ?? throw new InvalidOperationException("DbNull returned"));
        }

        return await info.GetResolution<T>(field).Converter.ReadAsync(reader, cancellationToken) ?? throw new InvalidOperationException("DbNull returned");
    }

    public override bool GetBoolean(int ordinal)
        => GetFieldValue<bool>(ordinal);

    public override byte GetByte(int ordinal)
        => GetFieldValue<byte>(ordinal);

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
        => GetFieldValue<char>(ordinal);

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override DateTime GetDateTime(int ordinal)
        => GetFieldValue<DateTime>(ordinal);

    public override decimal GetDecimal(int ordinal)
        => GetFieldValue<decimal>(ordinal);

    public override double GetDouble(int ordinal)
        => GetFieldValue<double>(ordinal);

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
    public override Type GetFieldType(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override float GetFloat(int ordinal)
        => GetFieldValue<float>(ordinal);

    public override Guid GetGuid(int ordinal)
        => GetFieldValue<Guid>(ordinal);

    public override short GetInt16(int ordinal)
        => GetFieldValue<short>(ordinal);

    public override int GetInt32(int ordinal)
        => GetFieldValue<int>(ordinal);

    public override long GetInt64(int ordinal)
        => GetFieldValue<long>(ordinal);

    public override string GetName(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetOrdinal(string name)
    {
        throw new NotImplementedException();
    }

    public override string GetString(int ordinal)
        => GetFieldValue<string>(ordinal);

    public override object GetValue(int ordinal)
        => GetFieldValue<object>(ordinal);

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

    public override void Close() => CloseCore(false).GetAwaiter().GetResult();
    protected override void Dispose(bool disposing) => DisposeCore(false).GetAwaiter().GetResult();

#if NETSTANDARD2_0
    public Task CloseAsync()
#else
    public override Task CloseAsync()
#endif
        => CloseCore(true).AsTask();

#if NETSTANDARD2_0
    public ValueTask DisposeAsync()
#else
    public override ValueTask DisposeAsync()
#endif
        => DisposeCore(true);
}

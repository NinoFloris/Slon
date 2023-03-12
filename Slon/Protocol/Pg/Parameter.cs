using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Slon.Pg;
using Slon.Pg.Types;

namespace Slon.Protocol.Pg;

readonly struct Parameter
{
    public Parameter(object? value, PgConverterInfo.Writer writer, ValueSize? size, DataRepresentation dataRepresentation = DataRepresentation.Binary, object? writeState = null)
    {
        Value = value;
        Writer = writer;
        Size = size;
        DataRepresentation = dataRepresentation;
        WriteState = writeState;
    }

    [MemberNotNullWhen(false, nameof(Size))]
    public bool IsDbNull => Size is null;

    /// Value can be an instance of IParameterSession or a direct parameter value.
    public object? Value { get; init; }
    /// Size set to null represents a db null.
    public ValueSize? Size { get; init; }
    public PgTypeId PgTypeId { get; init; }

    public PgConverterInfo.Writer Writer { get; init; }
    public object? WriteState { get; init; }

    public DataRepresentation DataRepresentation { get; init; }

    public bool TryGetParameterSession([NotNullWhen(true)]out IParameterSession? value)
    {
        if (Value is IParameterSession session)
        {
            value = session;
            return true;
        }

        value = null;
        return false;
    }
}

interface IBoxedParameterValueReader
{
    void ReadAsObject(object? value);
}

static class ParameterValueReaderExtensions
{
    public static void ReadParameterValue<TReader>(this ref TReader reader, object? value) where TReader : struct, IParameterValueReader, IBoxedParameterValueReader
    {
        if (value is IParameterSession session)
        {
            if (session.IsBoxedValue)
                reader.ReadAsObject(session.Value); // Just avoid the GVM call.
            else
                session.ApplyReader(ref reader);
        }
        else
            reader.ReadAsObject(value);
    }
}

static class PgConverterInfoExtensions
{
    public static Parameter CreateParameter(this PgConverterInfo converterInfo, object? parameterValue, int bufferLength, bool nullStructValueIsDbNull = true, DataRepresentation? preferredRepresentation = null)
    {
        var reader = new ValueReader(converterInfo, bufferLength, nullStructValueIsDbNull, preferredRepresentation);
        reader.ReadParameterValue(parameterValue);
        return new Parameter(parameterValue, reader.Writer, reader.Size, reader.Representation, reader.WriteState);
    }

    struct ValueReader: IParameterValueReader, IBoxedParameterValueReader
    {
        readonly PgConverterInfo _converterInfo;

        readonly int _bufferLength;
        readonly bool _nullStructValueIsDbNull;
        readonly DataRepresentation? _preferredRepresentation;
        public ValueSize? Size { get; private set; }
        DataRepresentation _representation;
        public DataRepresentation Representation => _representation;
        object? _writeState;
        public object? WriteState => _writeState;
        public PgConverterInfo.Writer Writer { get; private set; }

        public ValueReader(PgConverterInfo converterInfo, int bufferLength, bool nullStructValueIsDbNull, DataRepresentation? preferredRepresentation)
        {
            _converterInfo = converterInfo;
            _bufferLength = bufferLength;
            _nullStructValueIsDbNull = nullStructValueIsDbNull;
            _preferredRepresentation = preferredRepresentation;
        }

        public void Read<T>(T? value)
        {
            var writer = _converterInfo.GetWriter(value);
            Writer = writer.ToWriter();
            if (!writer.IsDbNullValue(value))
                Size = writer.GetAnySize(value, _bufferLength, out _writeState, out _representation, _preferredRepresentation);
        }

        public void ReadAsObject(object? value)
        {
            var writer = Writer = _converterInfo.GetWriter(value);
            if ((!_nullStructValueIsDbNull || value is not null) && !writer.IsDbNullValue(value))
                Size = writer.GetAnySize(value, _bufferLength, out _writeState, out _representation, _preferredRepresentation);
        }
    }
}

static class ParameterExtensions
{
    public static Oid GetOid(this Parameter parameter, PgTypeCatalog typeCatalog) => typeCatalog.GetOid(parameter.PgTypeId);

    public static void Write(this Parameter parameter, PgWriter writer)
    {
        if (writer.FlushMode is FlushMode.NonBlocking)
            ThrowNotSupported();

        if (parameter.IsDbNull)
            return;

        writer.DataRepresentation = parameter.DataRepresentation;
        var reader = new ValueWriter(writer, parameter.Writer, CancellationToken.None);
        reader.ReadParameterValue(parameter.Value);

        static void ThrowNotSupported() => throw new NotSupportedException("Cannot write with a non-blocking pgWriter.");
    }

    public static ValueTask WriteAsync(this Parameter parameter, PgWriter writer, CancellationToken cancellationToken)
    {
        if (writer.FlushMode is FlushMode.Blocking)
            ThrowNotSupported();

        if (parameter.IsDbNull)
            return new ValueTask();

        writer.DataRepresentation = parameter.DataRepresentation;
        var reader = new ValueWriter(writer, parameter.Writer, cancellationToken);
        reader.ReadParameterValue(parameter.Value);

        return reader.Result;

        static void ThrowNotSupported() => throw new NotSupportedException("Cannot write with a blocking pgWriter.");
    }

    public static BufferedOutput GetBufferedOutput(this Parameter parameter)
    {
        // TODO some array pool backed thing
        var pooledBufferWriter = (IBufferWriter<byte>)null!;
        var pgWriter = parameter.Writer.Info.Options.GetBufferedWriter(pooledBufferWriter, parameter.WriteState);
        pgWriter.DataRepresentation = parameter.HasTextWrite() ? DataRepresentation.Text : DataRepresentation.Binary;
        var reader = new ValueWriter(pgWriter, parameter.Writer, CancellationToken.None);
        return new BufferedOutput(default);
    }

    struct ValueWriter : IParameterValueReader, IBoxedParameterValueReader
    {
        readonly PgWriter _pgWriter;
        readonly PgConverterInfo.Writer _writer;
        readonly CancellationToken _cancellationToken;

        public ValueWriter(PgWriter pgWriter, PgConverterInfo.Writer writer, CancellationToken cancellationToken)
        {
            _pgWriter = pgWriter;
            _writer = writer;
            _cancellationToken = cancellationToken;
        }

        public ValueTask Result { get; private set; }

        public void Read<T>(T? value)
        {
            DebugShim.Assert(value is not null);
            var pgWriter = _pgWriter;
            if (pgWriter.FlushMode is not FlushMode.NonBlocking)
                _writer.ToWriter<T>().Write(pgWriter, value);
            else
                try
                {
                    Result = _writer.ToWriter<T>().WriteAsync(pgWriter, value, _cancellationToken);
                }
                catch (Exception ex)
                {
                    Result = new ValueTask(Task.FromException(ex));
                }
        }

        public void ReadAsObject(object? value)
        {
            DebugShim.Assert(value is not null);
            var pgWriter = _pgWriter;
            if (pgWriter.FlushMode is not FlushMode.NonBlocking)
                _writer.Write(pgWriter, value);
            else
                try
                {
                    Result = _writer.WriteAsync(pgWriter, value, _cancellationToken);
                }
                catch (Exception ex)
                {
                    Result = new ValueTask(Task.FromException(ex));
                }
        }
    }

    public static Type GetConverterType(this Parameter parameter)
        => parameter.Writer.Info.ConverterType;

    public static bool HasTextWrite(this Parameter parameter)
        => parameter.DataRepresentation is DataRepresentation.Text;
}

readonly struct PgTypeIdView
{
    readonly PooledMemory<Parameter> _parameters;
    readonly StructuralArray<PgTypeId> _pgTypeIds;
    readonly int _dataTypeNamesLength;

    public PgTypeIdView(ParameterContext parameterContext)
    {
        _parameters = parameterContext.Parameters;
    }

    public PgTypeIdView(StructuralArray<PgTypeId> pgTypeIds, int length)
    {
        if (pgTypeIds.Length > length)
            throw new ArgumentOutOfRangeException(nameof(length));

        _pgTypeIds = pgTypeIds;
        _dataTypeNamesLength = length;
    }

    public Enumerator GetEnumerator()
        => _pgTypeIds is { IsDefault: false }
        ? new(_pgTypeIds, _dataTypeNamesLength)
        : new(_parameters);

    public int Length => _parameters.Length;
    public bool IsEmpty => _parameters.Length == 0;

    public ref struct Enumerator
    {
        readonly ReadOnlySpan<Parameter> _parameters;
        readonly StructuralArray<PgTypeId> _pgTypeIds;
        readonly int _dataTypeNamesLength;
        PgTypeId _current;
        int _index;

        internal Enumerator(PooledMemory<Parameter> parameters)
        {
            _parameters = parameters.Span;
        }

        internal Enumerator(StructuralArray<PgTypeId> pgTypeIds, int length)
        {
            _pgTypeIds = pgTypeIds;
            _dataTypeNamesLength = length;
        }

        public bool MoveNext()
        {
            if (_pgTypeIds is { IsDefault: false } dataTypeNames)
            {
                if ((uint)_index < (uint)_dataTypeNamesLength)
                {
                    _current = dataTypeNames[_index];
                    _index++;
                    return true;
                }

                _current = default;
                _index = _dataTypeNamesLength + 1;
                return false;
            }
            else
            {
                var parameters = _parameters;
                if ((uint)_index < (uint)parameters.Length)
                {
                    _current = parameters[_index].PgTypeId;
                    _index++;
                    return true;
                }

                _current = default;
                _index = parameters.Length + 1;
                return false;
            }
        }

        public readonly PgTypeId Current => _current;

        public void Reset()
        {
            _index = 0;
            _current = default;
        }

        public void Dispose() { }
    }
}

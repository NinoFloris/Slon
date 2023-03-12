using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Slon.Pg;
using Slon.Pg.Types;

namespace Slon.Protocol.Pg;

// TODO subtract 4 per parameter from parameterBufferSize to account for length as well
struct ParameterContextBuilder
{
    readonly int _length;
    readonly int _parameterBufferSize;
    Parameter[]? Parameters { get; set; }
    int _remainingBufferSize;
    int _index;
    ParameterContextFlags _flags;

    public ParameterContextBuilder(int length, int parameterBufferSize, int revision, PgConverterOptions converterOptions)
    {
        _length = length;
        _parameterBufferSize = parameterBufferSize;
        Revision = revision;
        ConverterOptions = converterOptions;
        _remainingBufferSize = parameterBufferSize;
        _flags = ParameterContextFlags.None;
        NullStructValueIsDbNull = true;
    }

    public int Length => _length;
    public int Count => _index;
    public int Revision { get; }
    public PgConverterOptions ConverterOptions { get; }

    /// This property controls what should happen when a null value is passed for a non nullable struct type.
    /// This can happen if the boxed value is accompanied by a type id that resolves to a data type for a
    /// struct type (e.g. 'int').
    /// Parameter construction can be lenient and coerce these values to a db null when this property is true.
    /// When the property is false an exception is thrown during parameter construction.
    /// Default is true.
    public bool NullStructValueIsDbNull { get; set; }

    [MemberNotNull(nameof(Parameters))]
    void EnsureCapacity()
    {
        // Under failure we just let any rented builder arrays be collected by the GC.
        Parameters ??= ArrayPool<Parameter>.Shared.Rent(_length);
        if (_length <= _index)
            throw new IndexOutOfRangeException("Cannot add more parameters, builder was initialized with a length that has been reached.");
    }

    public ReadOnlySpan<Parameter> Items => new(Parameters, 0, _index);

    public Parameter AddParameter(Parameter parameter)
    {
        EnsureCapacity();
        return Parameters[_index++] = parameter;
    }

    public Parameter AddParameter(IParameterSession value, PgConverterInfo? converterInfo = null, DataRepresentation? preferredRepresentation = null)
        => AddParameterCore(value, pgTypeId: null, preferredRepresentation, converterInfo);

    public Parameter AddParameter(IParameterSession value, PgTypeId? pgTypeId = null, DataRepresentation? preferredRepresentation = null)
        => AddParameterCore(value, pgTypeId, preferredRepresentation, converterInfo: null);

    public Parameter AddParameter(object? value, PgConverterInfo? converterInfo = null, DataRepresentation? preferredRepresentation = null)
        => AddParameterCore(value, pgTypeId: null, preferredRepresentation, converterInfo);

    public Parameter AddParameter(object? value, PgTypeId? pgTypeId = null, DataRepresentation? preferredRepresentation = null)
        => AddParameterCore(value, pgTypeId, preferredRepresentation, converterInfo: null);

    Parameter AddParameterCore(object? value, PgTypeId? pgTypeId, DataRepresentation? preferredRepresentation, PgConverterInfo? converterInfo)
    {
        EnsureCapacity();
        var parameterKind = ParameterKind.Input;
        var isSession = false;
        Parameter parameter;
        if (value is IParameterSession session)
        {
            converterInfo ??= GetConverterInfo(ConverterOptions, session.ValueType, pgTypeId);
            parameter = converterInfo.CreateParameter(value, _remainingBufferSize, NullStructValueIsDbNull, preferredRepresentation: preferredRepresentation);
            parameterKind = session.Kind;
            isSession = true;
        }
        else
        {
            converterInfo ??= GetConverterInfo(ConverterOptions, value?.GetType(), pgTypeId);
            parameter = converterInfo.CreateParameter(value, _remainingBufferSize, NullStructValueIsDbNull, preferredRepresentation: preferredRepresentation);
        }

        if (parameter.Size is { Value: null })
        {
            // Disable optimizations for remaining parameters, as this is a parameter that needs buffering later on, we don't know what buffer remains.
            _remainingBufferSize = 0;
            _flags |= ParameterContextFlags.AnyUnknownByteCount;
        }
        else if (parameter.Size is { Value: null } size)
        {
            _remainingBufferSize -= size.Value ?? 0;

            if (size.Kind is ValueSizeKind.UpperBound && (_flags & ParameterContextFlags.AnyUpperBoundByteCount) == 0)
                _flags |= ParameterContextFlags.AnyUpperBoundByteCount;
        }

        if (isSession)
            _flags |= parameterKind is ParameterKind.Input ? ParameterContextFlags.AnySessions : ParameterContextFlags.AnySessions | ParameterContextFlags.AnyWritableParamSessions;

        if (parameter.DataRepresentation is not DataRepresentation.Binary)
            _flags |= ParameterContextFlags.AnyNonBinaryWrites;

        return Parameters[_index++] = parameter;
    }

    public ParameterContext Build()
    {
        // TODO if there are upper bounds the invariant should hold that all parameters should fit in a segment, otherwise they all need recomputing.
        // TODO to make lives easier we probably want to post process to prevent AnyUnknown and AnyUpperBound to exists together.
        // That means rerunning parameters that have AnyUpperBound with bufferlength 0 to get an exact size.
        // When we have both unknown byte count and upper bound byte counts we want to rerun the upper bounds to get an accurate value them.
        if ((_flags & (ParameterContextFlags.AnyUnknownByteCount | ParameterContextFlags.AnyUpperBoundByteCount)) == (ParameterContextFlags.AnyUnknownByteCount | ParameterContextFlags.AnyUpperBoundByteCount))
        {

        }

        var parameters = Parameters!;
        var length = _index;
        var remainingBufferSize = _remainingBufferSize;
        Parameters = null;
        _remainingBufferSize = _parameterBufferSize;
        _index = 0;
        return new ParameterContext
        {
            Parameters = new(parameters, length, pooledArray: true),
            Flags = _flags,
            MinimalBufferSegmentSize = _parameterBufferSize - remainingBufferSize
        };
    }

    static PgConverterInfo GetConverterInfo(PgConverterOptions options, Type? type, PgTypeId? typeId)
    {
        PgConverterInfo? converterInfo;
        if (type is null)
        {
            DebugShim.Assert(typeId is not null);
            converterInfo = options.GetDefaultConverterInfo(typeId.GetValueOrDefault());
        }
        else
        {
            DebugShim.Assert(type is not null);
            converterInfo = options.GetConverterInfo(type, typeId);
        }

        if (converterInfo is null)
            ThrowNotResolvable();

        return converterInfo;

        [DoesNotReturn]
        static void ThrowNotResolvable() => throw new InvalidOperationException("Cannot resolve converter info for parameter.");
    }
}

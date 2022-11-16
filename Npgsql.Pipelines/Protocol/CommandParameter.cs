using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Npgsql.Pipelines.Buffers;

namespace Npgsql.Pipelines.Protocol;

// Mirrors the System.Data enum
enum ParameterKind: byte
{
    Input = 1,
    Output = 2,
    InputOutput = 3,
    ReturnValue = 6
}

/// A class describing the parameter type and its other properties.
abstract class ParameterInfo
{
    /// Whether the parameter value is communicated in binary form.
    protected abstract bool GetIsBinary();
    public bool IsBinary => GetIsBinary();

    /// Available for fixed size parameters.
    protected abstract int? GetLength();
    public int? Length => GetLength();
}

interface IParameterSession
{
    public bool IsPositional { get; }
    public string Name { get; }
    ParameterKind Kind { get; }
    Facets Facets { get; }
    Type? Type { get; }
    object? Value { get; }

    /// Apply any output values, throws if its an input parameter.
    void ApplyOutput(object? value);
    /// Close the session once writes or reads from the session are finished, for instance once the protocol write is done.
    void Close();
}

interface IParameterSession<T>: IParameterSession
{
    new T? Value { get; }
    void ApplyOutput(T? value);
}

readonly struct CommandParameter
{
    readonly ParameterInfo _info;
    readonly object _value;
    readonly int? _length;

    public CommandParameter(ParameterInfo info, object value, int? length = null)
    {
        _info = info;
        _value = value;
        _length = length;
    }

    public ParameterInfo Info => _info;
    public ParameterKind Kind => _value is IParameterSession ep ? ep.Kind : ParameterKind.Input;
    public int? Length => _length;

    /// Value can be an instance of IParameterSession or a direct parameter value (or some custom behavior).
    public object Value => _value;

    public bool TryGetParameterSession([NotNullWhen(true)]out IParameterSession? value)
    {
        if (_value is IParameterSession session)
        {
            value = session;
            return true;
        }

        value = null;
        return false;
    }
}

readonly struct CommandParameters
{
    public ReadOnlyMemory<KeyValuePair<CommandParameter, ParameterWriter>> Collection { get; init; }
}

abstract class ParameterWriter
{
    public abstract void Write<T>(ref StreamingWriter<T> writer, CommandParameter parameter) where T : IStreamingWriter<byte>;
    public abstract void Write<T>(ref BufferWriter<T> writer, CommandParameter parameter) where T : IBufferWriter<byte>;
}

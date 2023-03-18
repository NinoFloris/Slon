using System;
using System.Collections.Generic;
using System.Data;
using Slon.Data;

namespace Slon;

// Base class for the two parameter types in Slon.
public abstract class SlonDbParameter : DbDataParameter, IParameterSession
{
    bool? _preferBinaryRepresentation;
    bool _inferredSlonDbType;
    bool _valueDependent;
    short _valueRevision;

    protected SlonDbParameter()
    { }
    protected SlonDbParameter(string parameterName)
        : base(parameterName)
    { }

    public new SlonDbParameter Clone() => (SlonDbParameter)CloneCore();

    // TODO should this be DataFormat?
    /// Some converters support both a textual and binary representation for the postgres type this parameter maps to.
    /// When this property is set to true a textual representation should be preferred.
    /// When its set to false a non-textual (binary) representation is preferred.
    /// The default value is null which allows the converter to pick the most optimal representation.
    public bool? PreferTextualRepresentation
    {
        get => _preferBinaryRepresentation;
        set
        {
            ThrowIfInUse();
            _preferBinaryRepresentation = value;
        }
    }

    public SlonDbType SlonDbType
    {
        get => SlonDbTypeCore;
        set
        {
            ThrowIfInUse();
            _inferredSlonDbType = false;
            SlonDbTypeCore = value;
        }
    }

    protected SlonDbType SlonDbTypeCore { get; set; }

    internal short ValueRevision => _valueRevision;

    internal Type? ValueType => ValueTypeCore;

    internal virtual bool ValueEquals(SlonDbParameter other) => Equals(ValueCore, other.ValueCore);

    internal void SetInferredDbType(SlonDbType slonDbType, bool valueDependent)
    {
        _inferredSlonDbType = true;
        _valueDependent = valueDependent;
        SlonDbTypeCore = slonDbType;
    }

    internal SlonDbType? GetExplicitDbType()
        => !_inferredSlonDbType && !SlonDbTypeCore.IsInfer ? SlonDbTypeCore : null;
    internal bool HasInferredSlonDbType => _inferredSlonDbType;

    Facets GetFacets(IFacetsTransformer? facetsTransformer = null)
    {
        if (Direction is ParameterDirection.Input)
            return new()
            {
                // We don't expect output so we leave IsNullable at default
                IsNullable = default,
                Size = SizeCore
            };

        return facetsTransformer is null ? GetUserSuppliedFacets() : GetFacetsCore(facetsTransformer);
    }

    private protected virtual Facets GetFacetsCore(IFacetsTransformer facetsTransformer)
    {
        if (ValueTypeCore is not null && ValueCore is not null)
            return facetsTransformer.Transform(ValueCore, ValueTypeCore, GetUserSuppliedFacets());

        return facetsTransformer.Transform(dbType: DbType, GetUserSuppliedFacets());
    }

    // Internal for now.
    private protected Facets GetUserSuppliedFacets() =>
        new()
        {
            IsNullable = IsNullable,
            Precision = PrecisionCore,
            Scale = ScaleCore,
            Size = SizeCore,
        };

    protected SlonDbParameter Clone(SlonDbParameter instance)
    {
        Clone((DbDataParameter)instance);
        instance.PreferTextualRepresentation = PreferTextualRepresentation;
        instance._inferredSlonDbType = true;
        instance._valueDependent = _valueDependent;
        instance.SlonDbType = SlonDbType;
        return instance;
    }

    protected sealed override void ResetInference()
    {
        base.ResetInference();
        if (_inferredSlonDbType)
        {
            _inferredSlonDbType = false;
            SlonDbTypeCore = SlonDbType.Infer;
            _valueDependent = false;
        }
    }

    protected void ValueUpdated(Type? previousType)
    {
        _valueRevision++;
        if (_valueDependent || (previousType is not null && previousType != ValueTypeCore))
            ResetInference();
    }

    internal abstract IParameterSession StartSession(IFacetsTransformer facetsTransformer);
    protected abstract void EndSession();
    protected abstract void SetSessionValue(object? value);

    internal static SlonParameter Create() => new();
    internal static SlonParameter Create(object? value) => new() { Value = value };
    internal static SlonParameter Create(string parameterName, object? value) => new(parameterName, value);
    internal static SlonParameter<T> Create<T>(T? value) => new() { Value = value };
    internal static SlonParameter<T> Create<T>(string parameterName, T? value) => new(parameterName, value);

    ParameterKind IParameterSession.Kind => (ParameterKind)Direction;
    Facets IParameterSession.Facets => GetFacets();
    Type? IParameterSession.ValueType => ValueTypeCore;
    bool IParameterSession.IsBoxedValue => true;
    string IParameterSession.Name => ParameterName;

    void IParameterSession.ApplyReader<TReader>(ref TReader reader) => reader.Read(Value);

    object? IParameterSession.Value
    {
        get => ValueCore;
        set => SetSessionValue(value);
    }
    void IParameterSession.Close() => EndSession();
}

public sealed class SlonParameter: SlonDbParameter
{
    object? _value;

    public SlonParameter() {}
    public SlonParameter(string parameterName, object? value)
        :base(parameterName)
    {
        // Make sure it goes through value update.
        Value = value;
    }

    public new SlonParameter Clone() => (SlonParameter)CloneCore();

    internal override IParameterSession StartSession(IFacetsTransformer facetsTransformer)
    {
        // TODO facets transformer should be used at this point or removed.

        if (IncrementInUse() > 1 && Direction is not ParameterDirection.Input)
        {
            DecrementInUse();
            throw new InvalidOperationException("An output or return value direction parameter can't be used by commands executing in parallel.");
        }

        return this;
    }

    protected override void EndSession() => DecrementInUse();

    protected override void SetSessionValue(object? value)
    {
        if (Direction is ParameterDirection.Input)
            throw new InvalidOperationException("Cannot change value of an input parameter.");
        ValueCore = value;
    }

    protected override object? ValueCore
    {
        get => _value;
        set
        {
            var previousType = ValueTypeCore;
            _value = value;
            ValueUpdated(previousType);
        }
    }

    protected override DbType? DbTypeCore { get; set; }
    protected override DbDataParameter CloneCore() => Clone(new SlonParameter { ValueCore = ValueCore });
}

public sealed class SlonParameter<T> : SlonDbParameter, IDbDataParameter<T>, IParameterSession<T>
{
    static readonly EqualityComparer<T> EqualityComparer = EqualityComparer<T>.Default;
    static readonly bool ImplementsIEquatable = typeof(IEquatable<>).IsAssignableFrom(typeof(T));

    public SlonParameter() {}
    public SlonParameter(T? value)
        :base(string.Empty)
        => Value = value;
    public SlonParameter(string parameterName, T? value)
        :base(parameterName)
        => Value = value;

    T? _value;

    public new T? Value
    {
        get => _value;
        set
        {
            ThrowIfInUse();
            SetValue(value);
        }
    }

    void SetValue(T? value)
    {
        _value = value;
        // We explicitly ignore any derived type polymorphism for the generic SlonParameter.
        // So an IEnumerable<T> parameter will stay IEnumerable<T> even though it's now backed by an array.
        ValueUpdated(ValueTypeCore);
    }

    public new SlonParameter<T> Clone() => (SlonParameter<T>)CloneCore();

    // TODO we need to think about structural equality here as we may think two values are equal while they were mutated over time (e.g. arrays)
    internal override bool ValueEquals(SlonDbParameter other)
    {
        if (other is SlonParameter<T> otherT)
            return EqualityComparer.Equals(_value!, otherT._value!);
        // At this point we could still find a T if a generic SlonParameter is instantiated at a derived type of T *or* if it's boxed on SlonParameter.
        // For value types the former is impossible while in the latter case its value is already a reference anyway.
        // Accordingly we never cause any per invocation boxing by calling other.Value here.
        if (other.Value is T valueT)
            return EqualityComparer.Equals(_value!, valueT);
        // Given any type its default default EqualityComparer, when a type implements IEquatable<T> its object equality is never consulted.
        // We do this ourselves so we won't have to box our value (JIT optimizes struct receivers calling their object inherited methods).
        // The worse alternative would be calling EqualityComparer.Equals(object?, object?) which boxes both sides.
        if (!ImplementsIEquatable && _value is not null)
            return _value.Equals(other.Value);

        return false;
    }

    internal override IParameterSession StartSession(IFacetsTransformer facetsTransformer)
    {
        // TODO facets transformer should write back the updated info (also when direction isn't input).

        if (IncrementInUse() > 1 && Direction is not ParameterDirection.Input)
        {
            DecrementInUse();
            throw new InvalidOperationException("An output or return value direction parameter can't be used by commands executing in parallel.");
        }

        return this;
    }

    protected override void EndSession() => DecrementInUse();
    protected override void SetSessionValue(object? value) => ((IParameterSession<T>)this).Value = (T?)value;

    private protected override Facets GetFacetsCore(IFacetsTransformer facetsTransformer)
        => Value is not null ? facetsTransformer.Transform<T>(Value, GetUserSuppliedFacets()) : facetsTransformer.Transform(dbType: DbType, GetUserSuppliedFacets());

    protected override Type? ValueTypeCore => typeof(T);
    protected override object? ValueCore { get => Value; set => Value = (T?)value; }
    protected override DbType? DbTypeCore { get; set; }

    protected override DbDataParameter CloneCore() => Clone(new SlonParameter<T> { Value = Value });

    bool IParameterSession.IsBoxedValue => false;
    void IParameterSession.ApplyReader<TReader>(ref TReader reader) => reader.Read(Value);
    T? IParameterSession<T>.Value
    {
        get => _value;
        set
        {
            if (Direction is ParameterDirection.Input)
                throw new InvalidOperationException("Cannot change value of an input parameter.");

            SetValue(value);
        }
    }
}

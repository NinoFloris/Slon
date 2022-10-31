using System;
using System.Data;
using Npgsql.Pipelines.Data;
using Npgsql.Pipelines.Protocol;

namespace Npgsql.Pipelines;

// Base class for the two parameter types in Npgsql.
public abstract class NpgsqlDbParameter : DbDataParameter, IParameterFacets, IParameterSession
{
    protected NpgsqlDbParameter()
    { }
    protected NpgsqlDbParameter(string parameterName)
        : base(parameterName)
    { }

    Facets IParameterFacets.GetFacets(IFacetsTransformer? facetsTransformer)
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
        if (ParameterType is not null && ValueCore is not null)
            return facetsTransformer.Invoke(ValueCore, ParameterType, GetUserSuppliedFacets());

        return facetsTransformer.Invoke(dbType: DbType, GetUserSuppliedFacets());
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

    internal new void NotifyCollectionAdd() => base.NotifyCollectionAdd();

    internal abstract IParameterSession StartSession(IFacetsTransformer facetsTransformer);
    protected abstract void EndSession();

    public static NpgsqlParameter Create() => new();
    public static NpgsqlParameter Create(object? value) => new() { Value = value };
    public static NpgsqlParameter Create(string parameterName, object? value) => new(parameterName, value);
    public static NpgsqlParameter<T> Create<T>(T? value) => new() { Value = value };
    public static NpgsqlParameter<T> Create<T>(string parameterName, T? value) => new(parameterName, value);

    bool IParameterSession.IsPositional => ParameterName is "";
    string IParameterSession.Name => ParameterName;
    ParameterKind IParameterSession.Kind => (ParameterKind)Direction;
    Facets IParameterSession.Facets => ((IParameterFacets)this).GetFacets();
    Type? IParameterSession.Type => ParameterType;
    void IParameterSession.ApplyOutput(object? value) => throw new NotSupportedException();
    void IParameterSession.Close() => EndSession();
}

public sealed class NpgsqlParameter: NpgsqlDbParameter
{
    public NpgsqlParameter() {}
    public NpgsqlParameter(string parameterName, object? value)
        :base(parameterName)
    {
        // Make sure it goes through value update.
        Value = value;
    }

    public new NpgsqlParameter Clone() => (NpgsqlParameter)base.Clone();

    internal override IParameterSession StartSession(IFacetsTransformer facetsTransformer)
    {
        if (Direction is not ParameterDirection.Input)
            return new Session(this, facetsTransformer);

        // We optimize Input parameters a bit by reusing its own instance.
        IncrementInUse();
        return this;
    }

    protected override void EndSession()
    {
        DecrementInUse();
    }

    protected override object? ValueCore { get; set; }
    protected override DbType DbTypeCore { get; set; }
    protected override DbDataParameter CloneCore() => new NpgsqlParameter { ValueCore = ValueCore };

    class Session : ParameterSession, IParameterSession
    {
        public Session(IDbDataParameter parameter, IFacetsTransformer facetsTransformer) : base(parameter, facetsTransformer)
        {
        }

        protected override object? ValueCore { get; init; }
        public ParameterKind Kind => (ParameterKind)Direction;
        public void ApplyOutput(object? value) => ChangeValue(value);
        public void Close()
        {
        }
    }
}

public sealed class NpgsqlParameter<T> : NpgsqlDbParameter, IDbDataParameter<T>
{
    public NpgsqlParameter() {}
    public NpgsqlParameter(string parameterName, T? value)
        :base(parameterName)
        => Value = value;

    T? _value;

    public new T? Value
    {
        get => _value;
        set
        {
            _value = value;
            // We explicitly ignore any derived type polymorphism for the generic NpgsqlParameter.
            // So an IEnumerable<T> parameter will stay IEnumerable<T> even though it's now backed by an array.
            ValueUpdated(ParameterType);
        }
    }
    public new NpgsqlParameter<T> Clone() => (NpgsqlParameter<T>)base.Clone();

    internal override IParameterSession StartSession(IFacetsTransformer facetsTransformer)
    {
        if (Direction is not ParameterDirection.Input)
            return new Session(this, facetsTransformer);

        // We optimize Input parameters a bit by reusing its own instance and ref counting uses.
        // An alternative would be to go the ICommand route and make a copy.
        // That feels too heavy when there may be many parameters, compared to doing that once per command as in the ICommand case.
        IncrementInUse();
        return this;
    }

    protected override void EndSession()
    {
        DecrementInUse();
    }

    private protected override Facets GetFacetsCore(IFacetsTransformer facetsTransformer)
        => Value is not null ? facetsTransformer.Invoke<T>(Value, GetUserSuppliedFacets()) : facetsTransformer.Invoke(dbType: DbType, GetUserSuppliedFacets());

    protected override Type ParameterType => typeof(T);
    protected override object? ValueCore { get => Value; set => Value = (T?)value; }
    protected override DbType DbTypeCore { get; set; }
    protected override DbDataParameter CloneCore() => new NpgsqlParameter<T> { Value = Value };

    class Session : ParameterSession<T>, IParameterSession<T>
    {
        public Session(IDbDataParameter<T> parameter, IFacetsTransformer facetsTransformer) : base(parameter, facetsTransformer)
        {
        }

        public ParameterKind Kind => (ParameterKind)Direction;
        public void ApplyOutput(object? value) => ChangeValue(value);
        public void ApplyOutput(T? value) => ChangeValue(value);
        public void Close()
        {
        }
    }
}

using System;
using System.Data;
using Npgsql.Pipelines.Data;
using Npgsql.Pipelines.Protocol;

namespace Npgsql.Pipelines;

// Base class for the two parameter types in Npgsql.
public abstract class NpgsqlDbParameter : DbDataParameter, IParameterSession
{
    protected NpgsqlDbParameter()
    { }
    protected NpgsqlDbParameter(string parameterName)
        : base(parameterName)
    { }

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

    internal abstract IParameterSession StartSession(IFacetsTransformer facetsTransformer);
    protected abstract void EndSession();

    internal static NpgsqlParameter Create() => new();
    internal static NpgsqlParameter Create(object? value) => new() { Value = value };
    internal static NpgsqlParameter Create(string parameterName, object? value) => new(parameterName, value);
    internal static NpgsqlParameter<T> Create<T>(T? value) => new() { Value = value };
    internal static NpgsqlParameter<T> Create<T>(string parameterName, T? value) => new(parameterName, value);

    string IParameterSession.Name => ParameterName;
    public bool IsBoxedValue => true;
    public Type? ValueType => ValueTypeCore;
    ParameterKind IParameterSession.Kind => (ParameterKind)Direction;
    Facets IParameterSession.Facets => GetUserSuppliedFacets();
    void IParameterSession.ApplyReader<TReader>(ref TReader reader)
    {
        throw new NotImplementedException();
    }

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
        IncrementInUse();
        return this;
    }

    protected override void EndSession()
    {
        DecrementInUse();
    }

    protected override object? ValueCore { get; set; }
    protected override DbType? DbTypeCore { get; set; }
    protected override DbDataParameter CloneCore() => new NpgsqlParameter { ValueCore = ValueCore };
}

public sealed class NpgsqlParameter<T> : NpgsqlDbParameter, IDbDataParameter<T>
{
    public NpgsqlParameter() {}
    public NpgsqlParameter(T? value)
        :base(string.Empty)
        => Value = value;
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
            // ValueUpdated(ValueTypeCore);
        }
    }
    public new NpgsqlParameter<T> Clone() => (NpgsqlParameter<T>)base.Clone();

    internal override IParameterSession StartSession(IFacetsTransformer facetsTransformer)
    {


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
        => Value is not null ? facetsTransformer.Transform<T>(Value, GetUserSuppliedFacets()) : facetsTransformer.Transform(dbType: DbType, GetUserSuppliedFacets());

    protected override Type? ValueTypeCore => typeof(T);
    protected override object? ValueCore { get => Value; set => Value = (T?)value; }
    protected override DbType? DbTypeCore { get; set; }
    protected override DbDataParameter CloneCore() => new NpgsqlParameter<T> { Value = Value };
}

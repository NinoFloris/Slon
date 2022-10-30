using System;
using System.Data;

namespace Npgsql.Pipelines.Data;

/// Interface for the purpose of augmenting and validating (potentially database specific) facet information.
interface IFacetsTransformer
{
    Facets Invoke(DbType dbType, Facets suppliedFacets);
    Facets Invoke<T>(T value, Facets suppliedFacets);
    Facets Invoke(object value, Type type, Facets suppliedFacets);
}

//     if (precision.HasValue)
//     {
//         // Be lax here, if we don't know the type that's ok.
//         if (facets.Precision.HasValue && precision.Value > facets.Precision.Value)
//         {
//             throw new ArgumentOutOfRangeException("Precision of this type of value cannot be larger than: " + facets.Precision.Value);
//         }
//     }
//
//     if (scale.HasValue)
//     {
//         // Be lax here, if we don't know the type that's ok.
//         if (facets.MaxScale.HasValue && scale.Value > facets.MaxScale.Value)
//         {
//             throw new ArgumentOutOfRangeException("Scale of this type of value cannot be larger than: " + facets.MaxScale.Value);
//         }
//     }


interface IParameterFacets
{
    Facets GetFacets(IFacetsTransformer? facetsTransformer = null);
}

/// Keeps a snapshot of DbParameter values from the start of execution and communicates outputs back to it.
abstract class ParameterSession
{
    readonly IDbDataParameter _instance;

    private protected ParameterSession(IDbDataParameter parameter, IFacetsTransformer? facetsTransformer, bool excludeTypeAndValue = true)
    {
        if (parameter is not IParameterFacets parameterFacets)
            throw new ArgumentException($"Parameter does not implement {nameof(IParameterFacets)}", nameof(parameter));

        var direction = parameter.Direction;
        if (direction == ParameterDirection.Input)
            throw new ArgumentException("Parameter should not have ParameterDirection.Input.", nameof(parameter));

        _instance = parameter;
        Name = parameter.ParameterName;
        Direction = direction;
        Facets = parameterFacets.GetFacets(facetsTransformer);
        if (excludeTypeAndValue)
        {
            Type = parameter.Value?.GetType();
            ValueCore = parameter.Value;
        }
    }

    protected ParameterSession(IDbDataParameter parameter, IFacetsTransformer? facetsTransformer)
        : this(parameter, facetsTransformer, excludeTypeAndValue: true)
    {}

    public ParameterDirection Direction { get; }
    public Facets Facets { get; }
    public bool IsPositional => Name is "";
    public string Name { get; }
    public Type? Type { get; protected init; }
    public object? Value => ValueCore;

    protected abstract object? ValueCore { get; init; }

    private protected void ChangeValue<TValue>(TValue value, Action<IDbDataParameter, TValue> setter) => setter(_instance, value);
    protected void ChangeValue(object? value) => _instance.Value = value;
}

abstract class ParameterSession<T> : ParameterSession
{
    protected ParameterSession(IDbDataParameter<T> parameter, IFacetsTransformer? facetsTransformer) : base(parameter, facetsTransformer, excludeTypeAndValue: false)
    {
        Type = typeof(T);
        Value = parameter.Value;
    }

    public new T? Value { get; protected init; }
    protected override object? ValueCore { get => Value; init => Value = (T?)value; }
    protected void ChangeValue(T? value)
        => ChangeValue(value, (p, v) => ((IDbDataParameter<T>)p).Value = (T?)v!);
}

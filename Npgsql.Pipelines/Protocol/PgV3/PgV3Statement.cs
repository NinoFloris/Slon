using System;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Npgsql.Pipelines.Pg.Types;
using Npgsql.Pipelines.Protocol.PgV3.Descriptors;

namespace Npgsql.Pipelines.Protocol.PgV3;

sealed class PgV3Statement: Statement
{
    int _isInvalid;
    int _uses;

    PgV3Statement(PreparationKind kind, bool isInvalid, StructuralArray<PgTypeId> parameterTypes, StructuralArray<StatementParameter> parameters, ImmutableArray<StatementField>? fields = null)
        : base(kind)
    {
        ParameterTypes = parameterTypes;
        Parameters = parameters;
        Fields = fields;
        _isInvalid = isInvalid ? 1 : 0;
    }

    PgV3Statement(PreparationKind kind, StructuralArray<PgTypeId> parameterTypes)
        : base(kind)
    {
        ParameterTypes = parameterTypes;
    }

    [MemberNotNullWhen(true, nameof(Fields))]
    public override bool IsComplete => Fields is not null;

    /// Statements become invalid after dropping off of the LRU list or when a pg backend error related to the preparation occurred.
    public bool IsInvalid => _isInvalid == 1;

    public int Uses => _uses;
    public int IncrementUses() => Interlocked.Increment(ref _uses);

    public void Invalidate()
    {
        if (IsInvalid)
            return;
        Interlocked.Exchange(ref _isInvalid, 1);
    }

    // Both StatementParameter and StatementField record oids which are not a backend agnostic way to refer to a statement.
    public bool IsBackendAgnosticStatement => Parameters.IsDefault;

    public StructuralArray<StatementParameter> Parameters { get; }
    public ImmutableArray<StatementField>? Fields { get; private set; }

    // Used in the first phase of a statement lifecycle, when it is purely a frontend instance.
    // This then transitions to one with concrete parameter oids filled in, and finally completes with fields being added.
    public StructuralArray<PgTypeId> ParameterTypes { get; }

    public void AddFields(ImmutableArray<StatementField> fields)
    {
        if (fields.IsDefault)
            ThrowDefaultGiven();

        Fields = fields;

        static void ThrowDefaultGiven() => throw new ArgumentException("Given value is a default value.", nameof(fields));
    }

    public static PgV3Statement CreateUnprepared(PreparationKind kind)
        => CreateUnprepared(kind, StructuralArray<PgTypeId>.Empty, StructuralArray<StatementParameter>.Empty);
    public static PgV3Statement CreateUnprepared(PreparationKind kind, StructuralArray<PgTypeId> parameterTypes, StructuralArray<StatementParameter> parameters)
        => new(kind, false, parameterTypes, parameters);

    public static PgV3Statement CreateDbAgnostic(PreparationKind kind, StructuralArray<PgTypeId> parameterTypes) => new(kind, parameterTypes);
}

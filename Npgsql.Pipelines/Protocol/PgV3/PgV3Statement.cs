using System.Diagnostics.CodeAnalysis;
using Npgsql.Pipelines.Protocol.PgV3.Descriptors;

namespace Npgsql.Pipelines.Protocol.PgV3;

record PgV3Statement: Statement
{
    [SetsRequiredMembers]
    protected PgV3Statement(PreparationKind kind, StructuralArray<Parameter>? parameters = null, StructuralArray<StatementField>? fields = null)
        : base(kind)
    {
        Kind = kind;
        Parameters = parameters;
        Fields = fields;
    }

    [MemberNotNullWhen(true, nameof(Parameters), nameof(Fields))]
    public override bool IsComplete => Parameters is not null && Fields is not null;

    public StructuralArray<Parameter>? Parameters { get; init; }
    public StructuralArray<StatementField>? Fields { get; init; }

    public static PgV3Statement CreateUnprepared(PreparationKind kind) => new(kind);
    public static PgV3Statement CreateUnprepared(PreparationKind kind, StructuralArray<Parameter> parameters) => new(kind, parameters);
}

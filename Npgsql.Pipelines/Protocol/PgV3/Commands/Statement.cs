using System;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Npgsql.Pipelines.Protocol.PgV3.Types;

namespace Npgsql.Pipelines.Protocol.PgV3.Commands;

enum PreparationKind
{
    /// An explicitly prepared statement.
    Command,
    /// An automatically prepared statement based on usage statistics.
    Auto,
    /// An explicitly prepared statement that applies to all connections.
    Global
}

// TODO Somehow allow a handle to this to be passed into a npgsqlcommand (maybe just the id in a struct, or as an object reference)
record Statement
{
    [SetsRequiredMembers]
    Statement(PreparationKind kind, ImmutableArray<Parameter>? parameters = null, ImmutableArray<Field>? fields = null)
    {
        Kind = kind;
        Parameters = parameters;
        Fields = fields;
    }

    public Guid Id { get; } = Guid.NewGuid();

    [MemberNotNullWhen(true, nameof(Parameters), nameof(Fields))]
    public bool IsComplete => Fields is not null;

    public required PreparationKind Kind { get; init; }
    public ImmutableArray<Parameter>? Parameters { get; init; }
    public ImmutableArray<Field>? Fields { get; init; }

    public static Statement CreateUnprepared(PreparationKind kind) => new(kind);
    public static Statement CreateUnprepared(PreparationKind kind, ImmutableArray<Parameter> parameters) => new(kind, parameters);
}

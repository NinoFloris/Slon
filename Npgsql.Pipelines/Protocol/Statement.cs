using System;
using System.Diagnostics.CodeAnalysis;

namespace Npgsql.Pipelines.Protocol;

enum PreparationKind
{
    /// An explicitly prepared statement.
    Command,
    /// An automatically prepared statement based on usage statistics.
    Auto,
    /// An explicitly prepared statement that applies to all connections.
    Global
}

abstract record Statement
{
    [SetsRequiredMembers]
    protected Statement(PreparationKind kind)
    {
        Kind = kind;
    }

    // Whether the statement has gone through all the required operations to be used to run matching commands in a prepared fashion.
    public abstract bool IsComplete { get; }
    public Guid Id { get; } = Guid.NewGuid();
    public required PreparationKind Kind { get; init; }
}

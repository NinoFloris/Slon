using Npgsql.Pipelines.Protocol.PgV3.Commands;

namespace Npgsql.Pipelines.Protocol;

interface ICommandSession
{
    public Statement? Statement { get; }

    /// <remarks>
    /// Flags can change until the first ReadMessage{Async} has been serviced
    /// For instance due to the multiplexing loop being slow while the read slot is activated right away.
    /// </remarks>
    public ExecutionFlags ExecutionFlags { get; }

    /// <summary>
    /// Invoked after the statement has been successfully prepared, providing backend information about the statement.
    /// </summary>
    /// <param name="statement"></param>
    /// <exception cref="System.NotSupportedException">
    /// Thrown when <see cref="ICommandSession.ExecutionFlags">ExecutionFlags</see> does not have <see cref="ExecutionFlags.Preparing">Preparing</see> set.
    /// </exception>
    public void CompletePreparation(Statement statement);
}

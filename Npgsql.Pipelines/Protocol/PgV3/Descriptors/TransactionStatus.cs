namespace Npgsql.Pipelines.Protocol.PgV3.Descriptors;

enum TransactionStatus : byte
{
    /// <summary>
    /// Currently not in a transaction block
    /// </summary>
    Idle = (byte)'I',

    /// <summary>
    /// Currently in a transaction block
    /// </summary>
    InTransactionBlock = (byte)'T',

    /// <summary>
    /// Currently in a failed transaction block (queries will be rejected until block is ended)
    /// </summary>
    InFailedTransactionBlock = (byte)'E',
}

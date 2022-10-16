using System.IO;
using System.Runtime.CompilerServices;

namespace Npgsql.Pipelines.Protocol.PgV3;

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

struct ReadyForQuery: IPgV3BackendMessage
{
    TransactionStatus _transactionStatus;
    TransactionStatus TransactionStatus => _transactionStatus;

    public ReadStatus Read(ref MessageReader<PgV3Header> reader)
    {
        if (!reader.MoveNextAndIsExpected(BackendCode.ReadyForQuery, out var status, ensureBuffered: true))
            return status;

        reader.TryReadByte(out Unsafe.As<TransactionStatus, byte>(ref _transactionStatus));
        if (BackendMessage.DebugEnabled && !EnumShim.IsDefined(_transactionStatus))
            throw new InvalidDataException("Unknown transaction status: " + _transactionStatus);

        reader.ConsumeCurrent();
        return ReadStatus.Done;
    }
}

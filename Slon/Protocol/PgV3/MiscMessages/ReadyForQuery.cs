using System;
using System.Runtime.CompilerServices;
using Slon.Protocol.PgV3.Descriptors;

namespace Slon.Protocol.PgV3;

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
            throw new ArgumentOutOfRangeException(nameof(_transactionStatus), _transactionStatus, "Unknown value.");

        reader.ConsumeCurrent();
        return ReadStatus.Done;
    }
}

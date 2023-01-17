namespace Slon.Protocol.PgV3;

readonly struct CloseComplete: IPgV3BackendMessage
{
    public ReadStatus Read(ref MessageReader<PgV3Header> reader)
    {
        if (!reader.MoveNextAndIsExpected(BackendCode.CloseComplete, out var status, ensureBuffered: true))
            return status;

        return ReadStatus.Done;
    }
}

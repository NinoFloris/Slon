namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct ParseComplete: IPgV3BackendMessage
{
    public ReadStatus Read(ref MessageReader<PgV3Header> reader)
    {
        if (!reader.MoveNextAndIsExpected(BackendCode.ParseComplete, out var status, ensureBuffered: true))
            return status;

        reader.ConsumeCurrent();
        return ReadStatus.Done;
    }
}

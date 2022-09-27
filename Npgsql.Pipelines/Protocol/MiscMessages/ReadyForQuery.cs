namespace Npgsql.Pipelines.MiscMessages;

struct ReadyForQuery: IBackendMessage
{
    public ReadStatus Read(ref MessageReader reader)
    {
        if (!reader.MoveNextAndIsExpected(BackendCode.ReadyForQuery, out var status, ensureBuffered: true))
            return status;

        reader.ConsumeCurrent();
        return ReadStatus.Done;
    }
}

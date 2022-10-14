namespace Npgsql.Pipelines.Protocol;

readonly struct ParseComplete: IBackendMessage
{
    public ReadStatus Read(ref MessageReader reader)
    {
        if (!reader.MoveNextAndIsExpected(BackendCode.ParseComplete, out var status, ensureBuffered: true))
            return status;

        reader.ConsumeCurrent();
        return ReadStatus.Done;
    }
}

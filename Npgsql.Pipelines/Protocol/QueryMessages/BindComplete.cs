namespace Npgsql.Pipelines.Protocol;

readonly struct BindComplete: IBackendMessage
{
    public ReadStatus Read(ref MessageReader reader)
    {
        if (!reader.MoveNextAndIsExpected(BackendCode.BindComplete, out var status, ensureBuffered: true))
            return status;

        reader.ConsumeCurrent();
        return ReadStatus.Done;
    }
}

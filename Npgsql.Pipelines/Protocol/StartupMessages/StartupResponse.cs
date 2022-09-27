namespace Npgsql.Pipelines.StartupMessages;

struct StartupResponse : IBackendMessage
{
    bool atRfq;

    public ReadStatus Read(ref MessageReader reader)
    {
        if (atRfq)
            goto atRfq;

        if (!reader.MoveNext())
            return ReadStatus.NeedMoreData;

        if (!reader.SkipSimilar(BackendCode.ParameterStatus, out var status))
            return status;

        if (!reader.IsExpected(BackendCode.BackendKeyData, out status))
            return status;

        atRfq = true;

        atRfq:
        if (!reader.MoveNextAndIsExpected(BackendCode.ReadyForQuery, out status, ensureBuffered: true))
            return status;

        // Don't try to read the next message as we won't have more to read, just consume.
        reader.ConsumeCurrent();
        return ReadStatus.Done;
    }
}

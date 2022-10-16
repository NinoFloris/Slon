namespace Npgsql.Pipelines.Protocol.PgV3;

struct StartupResponse : IPgV3BackendMessage
{
    bool atRfq;

    public ReadStatus Read(ref MessageReader<PgV3Header> reader)
    {
        if (atRfq)
            goto atRfq;

        if (!reader.MoveNext())
            return ReadStatus.NeedMoreData;

        if (!reader.SkipSimilar(BackendCode.ParameterStatus, out var status))
            return status;

        // TODO read backendkeydata for cancellation
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

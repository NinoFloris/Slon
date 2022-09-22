namespace Npgsql.Pipelines.StartupMessages;

struct StartupResponse : IBackendMessage
{
    int _state;

    public ReadStatus Read(ref MessageReader reader)
    {
        switch (_state)
        {
            case 1: goto state1;
            case 2: goto state2;
        }

        if (!reader.MoveNext())
            return ReadStatus.NeedMoreData;

        if (!reader.SkipSimilar(BackendCode.ParameterStatus))
            return ReadStatus.NeedMoreData;

        // As this is the startup we don't *actually* have to do this but... (all other similar readers would have to)
        // We may end up on an async response here before getting more parameter statuses, so check.
        // if (!reader.IsExpected(BackendCode.BackendKeyData, out var status))
        //     return status;

        _state = 1;

state1:
        if (!reader.IsExpected(BackendCode.BackendKeyData, out var status))
            return status;
        _state = 2;

state2:
        if (!reader.MoveNextAndIsExpected(BackendCode.ReadyForQuery, out status, ensureBuffered: true))
            return status;

        // Don't try to read the next message as we won't have more to read, just consume.
        reader.ConsumeCurrent();
        return ReadStatus.Done;
    }
}

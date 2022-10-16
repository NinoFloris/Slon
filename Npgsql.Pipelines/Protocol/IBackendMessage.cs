namespace Npgsql.Pipelines.Protocol;

static class BackendMessage {
    public static readonly bool DebugEnabled = false;
}

public enum ReadStatus
{
    Done,
    ConsumeData,
    NeedMoreData,
    InvalidData, // Desync
    AsyncResponse
}

interface IBackendMessage<THeader> where THeader : struct, IHeader<THeader>
{
    ReadStatus Read(ref MessageReader<THeader> reader);
}

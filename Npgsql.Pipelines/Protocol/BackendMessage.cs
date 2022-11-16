using System;
using System.Buffers;

namespace Npgsql.Pipelines.Protocol;

static class BackendMessage {
    public static readonly bool DebugEnabled = false;
}

enum ReadStatus
{
    Done,
    ConsumeData,
    NeedMoreData,
    InvalidData, // Desync
    AsyncResponse
}

static class ReadStatusExtensions
{
    public static ReadStatus ToReadStatus(this OperationStatus status)
        => status switch
        {
            OperationStatus.Done => ReadStatus.Done,
            OperationStatus.NeedMoreData => ReadStatus.NeedMoreData,
            OperationStatus.InvalidData => ReadStatus.InvalidData,
            _ => throw new ArgumentOutOfRangeException(nameof(status), status, null)
        };
}

interface IBackendMessage<THeader> where THeader : struct, IHeader<THeader>
{
    ReadStatus Read(ref MessageReader<THeader> reader);
}

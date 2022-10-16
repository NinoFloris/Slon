using System.Runtime.CompilerServices;

namespace Npgsql.Pipelines.Protocol.PgV3;

static class MessageReaderExtensions
{
    /// <summary>
    /// Skip messages until header.Code does not equal code.
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="code"></param>
    /// <param name="matchingHeader"></param>
    /// <param name="status">returned if we found an async response or couldn't skip past 'code' yet.</param>
    /// <returns>Returns true if skip succeeded, false if it could not skip past 'code' yet.</returns>
    public static bool SkipSimilar(this ref MessageReader<PgV3Header> reader, BackendCode code, out ReadStatus status)
        => reader.SkipSimilar(PgV3Header.CreateType(code), out status);

    public static bool IsExpected(this ref MessageReader<PgV3Header> reader, BackendCode code, out ReadStatus status, bool ensureBuffered = false)
        => reader.IsExpected(PgV3Header.CreateType(code), out status, ensureBuffered);

    public static bool MoveNextAndIsExpected(this ref MessageReader<PgV3Header> reader, BackendCode code, out ReadStatus status, bool ensureBuffered = false)
        => reader.MoveNextAndIsExpected(PgV3Header.CreateType(code), out status, ensureBuffered);

    public static bool ReadMessage<T>(this ref MessageReader<PgV3Header> reader, out ReadStatus status)
        where T : struct, IBackendMessage<PgV3Header>
    {
        status = new T().Read(ref reader);
        return status == ReadStatus.Done;
    }

}

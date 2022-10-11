using System;
using System.Buffers;
using System.Buffers.Text;

namespace Npgsql.Pipelines.QueryMessages;

struct CommandComplete: IBackendMessage
{
    public StatementType StatementType { get; private set; }
    public Oid Oid { get; private set; }
    public ulong Rows { get; private set; }

    public ReadStatus Read(ref MessageReader reader)
    {
        if (!reader.MoveNextAndIsExpected(BackendCode.CommandComplete, out var status, ensureBuffered: true))
            return status;

        if (!reader.TryReadCStringBuffer(out ReadOnlySequence<byte> buffer))
            return ReadStatus.InvalidData;

        // 64 bytes easily fits anything coming back from PostgreSQL, including "INSERT 2147483647 18446744073709551615" or "CREATE TABLE AS 18446744073709551615"
        // The amount of rows there (ulong.MaxValue) cannot even be returned in practice given the default BLCKSZ (see https://www.postgresql.org/docs/current/limits.html).
        const int stackallocByteThreshold = 64;
        var bytes = reader.CurrentRemaining <= stackallocByteThreshold ? stackalloc byte[64] : new byte[reader.CurrentRemaining];
        buffer.CopyTo(bytes);

        (StatementType, var argumentsStart) = Convert.ToChar(bytes[0]) switch
        {
            'I' when bytes.StartsWith("INSERT "u8) => (StatementType.Insert, "INSERT ".Length),
            'D' when bytes.StartsWith("DELETE "u8) => (StatementType.Delete, "DELETE ".Length),
            'U' when bytes.StartsWith("UPDATE "u8) => (StatementType.Update, "UPDATE ".Length),
            'S' when bytes.StartsWith("SELECT "u8) => (StatementType.Select, "SELECT ".Length),
            'C' when bytes.StartsWith("CREATE TABLE AS "u8) => (StatementType.CreateTableAs, "CREATE TABLE AS ".Length),
            'M' when bytes.StartsWith("MERGE "u8) => (StatementType.Merge, "MERGE ".Length),
            'M' when bytes.StartsWith("MOVE "u8) => (StatementType.Move, "MOVE ".Length),
            'F' when bytes.StartsWith("FETCH "u8) => (StatementType.Fetch, "FETCH ".Length),
            'C' when bytes.StartsWith("COPY "u8) => (StatementType.Copy, "COPY ".Length),
            _ => (StatementType.Other, 0)
        };

        var arguments = bytes.Slice(argumentsStart);
        switch (StatementType)
        {
            case StatementType.Other:
                break;
            case StatementType.Insert:
                if (!Utf8Parser.TryParse(arguments, out int oid, out var nextArgumentOffset))
                    return ReadStatus.InvalidData;
                Oid = new Oid(oid);
                arguments = bytes.Slice(nextArgumentOffset);
                goto default;
            default:
                if (!Utf8Parser.TryParse(arguments, out ulong rows, out _))
                    return ReadStatus.InvalidData;
                Rows = rows;
                break;
        }

        reader.ConsumeCurrent();
        return ReadStatus.Done;
    }
}

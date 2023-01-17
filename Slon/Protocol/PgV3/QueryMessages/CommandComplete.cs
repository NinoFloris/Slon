using System;
using System.Buffers;
using System.Buffers.Text;
using Slon.Pg.Types;

namespace Slon.Protocol.PgV3;

struct CommandComplete: IPgV3BackendMessage
{
    public StatementType StatementType { get; private set; }
    public Oid Oid { get; private set; }
    public ulong Rows { get; private set; }

    public ReadStatus Read(ref MessageReader<PgV3Header> reader)
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

        // PostgreSQL always writes these strings as ASCII, see https://github.com/postgres/postgres/blob/c8e1ba736b2b9e8c98d37a5b77c4ed31baf94147/src/backend/tcop/cmdtag.c#L130-L133
        (StatementType, var argumentsStart) = Convert.ToChar(bytes[0]) switch
        {
            'S' when bytes.StartsWith("SELECT "u8) => (StatementType.Select, "SELECT ".Length),
            'I' when bytes.StartsWith("INSERT "u8) => (StatementType.Insert, "INSERT ".Length),
            'U' when bytes.StartsWith("UPDATE "u8) => (StatementType.Update, "UPDATE ".Length),
            'D' when bytes.StartsWith("DELETE "u8) => (StatementType.Delete, "DELETE ".Length),
            'M' when bytes.StartsWith("MERGE "u8) => (StatementType.Merge, "MERGE ".Length),
            'C' when bytes.StartsWith("COPY "u8) => (StatementType.Copy, "COPY ".Length),
            'C' when bytes.StartsWith("CALL "u8) => (StatementType.Call, "CALL ".Length),
            'M' when bytes.StartsWith("MOVE "u8) => (StatementType.Move, "MOVE ".Length),
            'F' when bytes.StartsWith("FETCH "u8) => (StatementType.Fetch, "FETCH ".Length),
            'C' when bytes.StartsWith("CREATE TABLE AS "u8) => (StatementType.CreateTableAs, "CREATE TABLE AS ".Length),
            _ => (StatementType.Other, 0)
        };

        var arguments = bytes.Slice(argumentsStart);
        switch (StatementType)
        {
            case StatementType.Other:
                break;
            case StatementType.Insert:
                if (!Utf8Parser.TryParse(arguments, out uint oid, out var nextArgumentOffset))
                    return ReadStatus.InvalidData;
                Oid = new(oid);
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

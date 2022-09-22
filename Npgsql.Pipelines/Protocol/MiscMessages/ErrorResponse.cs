using System;
using System.Buffers;
using System.Diagnostics;

namespace Npgsql.Pipelines.MiscMessages;

struct ErrorResponse: IBackendMessage
{
    public ReadStatus Read(ref MessageReader reader)
    {
        throw new System.NotImplementedException();
    }
}

class ErrorOrNoticeMessage
{
    internal string Severity { get; }
    internal string InvariantSeverity { get; }
    internal string SqlState { get; }
    internal string Message { get; }
    internal string? Detail { get; }
    internal string? Hint { get; }
    internal int Position { get; }
    internal int InternalPosition { get; }
    internal string? InternalQuery { get; }
    internal string? Where { get; }
    internal string? SchemaName { get; }
    internal string? TableName { get; }
    internal string? ColumnName { get; }
    internal string? DataTypeName { get; }
    internal string? ConstraintName { get; }
    internal string? File { get; }
    internal string? Line { get; }
    internal string? Routine { get; }

    internal static ErrorOrNoticeMessage Load(ref MessageReader reader)
    {
        (string? severity, string? invariantSeverity, string? code, string? message, string? detail, string? hint) = (null, null, null, null, null, null);
        var (position, internalPosition) = (0, 0);
        (string? internalQuery, string? where) = (null, null);
        (string? schemaName, string? tableName, string? columnName, string? dataTypeName, string? constraintName) =
            (null, null, null, null, null);
        (string? file, string? line, string? routine) = (null, null, null);

        ref var sq = ref reader.Reader;

        while (true)
        {
            var read = sq.TryRead(out var fieldCodeByte);
            Debug.Assert(read);
            var fieldCode = (ErrorFieldTypeCode) fieldCodeByte;

            switch (fieldCode)
            {
                case ErrorFieldTypeCode.Done:
                    // Null terminator; error message fully consumed.
                    goto End;
                case ErrorFieldTypeCode.Severity:
                    reader.TryReadCString(out severity);
                    break;
                case ErrorFieldTypeCode.InvariantSeverity:
                    reader.TryReadCString(out invariantSeverity);
                    break;
                case ErrorFieldTypeCode.Code:
                    reader.TryReadCString(out code);
                    break;
                case ErrorFieldTypeCode.Message:
                    reader.TryReadCString(out message);
                    break;
                case ErrorFieldTypeCode.Detail:
                    reader.TryReadCString(out detail);
                    break;
                case ErrorFieldTypeCode.Hint:
                    reader.TryReadCString(out hint);
                    break;
                case ErrorFieldTypeCode.Position:
                    reader.TryReadCString(out var positionStr);
                    if (!int.TryParse(positionStr, out var tmpPosition))
                    {
                        continue;
                    }
                    position = tmpPosition;
                    break;
                case ErrorFieldTypeCode.InternalPosition:
                    reader.TryReadCString(out var internalPositionStr);
                    if (!int.TryParse(internalPositionStr, out var internalPositionTmp))
                    {
                        continue;
                    }
                    internalPosition = internalPositionTmp;
                    break;
                case ErrorFieldTypeCode.InternalQuery:
                    reader.TryReadCString(out internalQuery);
                    break;
                case ErrorFieldTypeCode.Where:
                    reader.TryReadCString(out where);
                    break;
                case ErrorFieldTypeCode.File:
                    reader.TryReadCString(out file);
                    break;
                case ErrorFieldTypeCode.Line:
                    reader.TryReadCString(out line);
                    break;
                case ErrorFieldTypeCode.Routine:
                    reader.TryReadCString(out routine);
                    break;
                case ErrorFieldTypeCode.SchemaName:
                    reader.TryReadCString(out schemaName);
                    break;
                case ErrorFieldTypeCode.TableName:
                    reader.TryReadCString(out tableName);
                    break;
                case ErrorFieldTypeCode.ColumnName:
                    reader.TryReadCString(out columnName);
                    break;
                case ErrorFieldTypeCode.DataTypeName:
                    reader.TryReadCString(out dataTypeName);
                    break;
                case ErrorFieldTypeCode.ConstraintName:
                    reader.TryReadCString(out constraintName);
                    break;
                default:
                    // Unknown error field; consume and discard.
                    reader.TryReadCString(out _);
                    break;
            }
        }

        End:
        if (severity == null)
            throw new Exception("Severity not received in server error message");
        if (code == null)
            throw new Exception("Code not received in server error message");
        if (message == null)
            throw new Exception("Message not received in server error message");

        return new ErrorOrNoticeMessage(
            severity, invariantSeverity ?? severity, code, message,
            detail, hint, position, internalPosition, internalQuery, where,
            schemaName, tableName, columnName, dataTypeName, constraintName,
            file, line, routine);

    }

    internal ErrorOrNoticeMessage(
        string severity, string invariantSeverity, string sqlState, string message,
        string? detail = null, string? hint = null, int position = 0, int internalPosition = 0, string? internalQuery = null, string? where = null,
        string? schemaName = null, string? tableName = null, string? columnName = null, string? dataTypeName = null, string? constraintName = null,
        string? file = null, string? line = null, string? routine = null)
    {
        Severity = severity;
        InvariantSeverity = invariantSeverity;
        SqlState = sqlState;
        Message = message;
        Detail = detail;
        Hint = hint;
        Position = position;
        InternalPosition = internalPosition;
        InternalQuery = internalQuery;
        Where = where;
        SchemaName = schemaName;
        TableName = tableName;
        ColumnName = columnName;
        DataTypeName = dataTypeName;
        ConstraintName = constraintName;
        File = file;
        Line = line;
        Routine = routine;
    }

    /// <summary>
    /// Error and notice message field codes
    /// </summary>
    internal enum ErrorFieldTypeCode : byte
    {
        Done = 0,
        Severity = (byte) 'S',
        InvariantSeverity = (byte) 'V',
        Code = (byte) 'C',
        Message = (byte) 'M',
        Detail = (byte) 'D',
        Hint = (byte) 'H',
        Position = (byte) 'P',
        InternalPosition = (byte) 'p',
        InternalQuery = (byte) 'q',
        Where = (byte) 'W',
        SchemaName = (byte) 's',
        TableName = (byte) 't',
        ColumnName = (byte) 'c',
        DataTypeName = (byte) 'd',
        ConstraintName = (byte) 'n',
        File = (byte) 'F',
        Line = (byte) 'L',
        Routine = (byte) 'R'
    }
}

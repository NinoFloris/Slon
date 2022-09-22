namespace Npgsql.Pipelines.MiscMessages;

struct ErrorResponse: IBackendMessage
{
    /// <summary>
    /// Error and notice message field codes
    /// </summary>
    enum ErrorFieldTypeCode : byte
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

    public ErrorOrNoticeMessage ErrorOrNoticeMessage { get; private set; }

    public ReadStatus Read(ref MessageReader reader)
    {
        (string? severity, string? invariantSeverity, string? code, string? message, string? detail, string? hint) = (null, null, null, null, null, null);
        var (position, internalPosition) = (0, 0);
        (string? internalQuery, string? where) = (null, null);
        (string? schemaName, string? tableName, string? columnName, string? dataTypeName, string? constraintName) =
            (null, null, null, null, null);
        (string? file, string? line, string? routine) = (null, null, null);

        if (!reader.MoveNextAndIsExpected(BackendCode.ErrorResponse, out var status, ensureBuffered: true))
            return status;

        ref var sq = ref reader.Reader;

        var fin = false;
        while (!fin)
        {
            sq.TryRead(out var fieldCodeByte);
            var fieldCode = (ErrorFieldTypeCode) fieldCodeByte;

            switch (fieldCode)
            {
                case ErrorFieldTypeCode.Done:
                    // Null terminator; error message fully consumed.
                    fin = true;
                    break;
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

        if (severity == null || code == null || message == null)
            return ReadStatus.InvalidData;

        ErrorOrNoticeMessage = new ErrorOrNoticeMessage(
            severity, invariantSeverity ?? severity, code, message,
            detail, hint, position, internalPosition, internalQuery, where,
            schemaName, tableName, columnName, dataTypeName, constraintName,
            file, line, routine);
        return ReadStatus.Done;
    }
}

class ErrorOrNoticeMessage
{
    public string Severity { get; }
    public string InvariantSeverity { get; }
    public string SqlState { get; }
    public string Message { get; }
    public string? Detail { get; }
    public string? Hint { get; }
    public int Position { get; }
    public int InternalPosition { get; }
    public string? InternalQuery { get; }
    public string? Where { get; }
    public string? SchemaName { get; }
    public string? TableName { get; }
    public string? ColumnName { get; }
    public string? DataTypeName { get; }
    public string? ConstraintName { get; }
    public string? File { get; }
    public string? Line { get; }
    public string? Routine { get; }

    public ErrorOrNoticeMessage(
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
}

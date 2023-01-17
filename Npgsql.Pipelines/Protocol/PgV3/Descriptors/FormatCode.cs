using System;
using Npgsql.Pipelines.Pg;

namespace Npgsql.Pipelines.Protocol.PgV3.Descriptors;

enum FormatCode: short
{
    Text = 0,
    Binary = 1
}

static class DataTypeRepresentationExtensions
{
    public static FormatCode ToFormatCode(this DataRepresentation repr)
        => repr switch
        {
            DataRepresentation.Binary => FormatCode.Binary,
            DataRepresentation.Text => FormatCode.Text,
            _ => throw new ArgumentOutOfRangeException(nameof(repr), repr, null)
        };
}

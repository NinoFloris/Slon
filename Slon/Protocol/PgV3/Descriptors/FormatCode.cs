using System;
using Slon.Pg;

namespace Slon.Protocol.PgV3.Descriptors;

enum FormatCode: short
{
    Text = 0,
    Binary = 1
}

static class DataTypeRepresentationExtensions
{
    public static FormatCode ToFormatCode(this DataFormat repr)
        => repr switch
        {
            DataFormat.Binary => FormatCode.Binary,
            DataFormat.Text => FormatCode.Text,
            _ => throw new ArgumentOutOfRangeException(nameof(repr), repr, null)
        };
}

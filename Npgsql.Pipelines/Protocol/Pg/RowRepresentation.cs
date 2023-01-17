using System;
using Npgsql.Pipelines.Pg;

namespace Npgsql.Pipelines.Protocol.Pg;

readonly struct RowRepresentation
{
    RowRepresentation(DataRepresentation repr) => ForAll = repr;
    RowRepresentation(ReadOnlyMemory<DataRepresentation> reprs) => PerColumn = reprs;

    public bool IsForAll => PerColumn.IsEmpty;
    public bool IsPerColumn => !IsForAll;

    public DataRepresentation ForAll { get; }
    public ReadOnlyMemory<DataRepresentation> PerColumn { get; }

    public static RowRepresentation CreateForAll(DataRepresentation code) => new(code);
    public static RowRepresentation CreatePerColumn(ReadOnlyMemory<DataRepresentation> codes) => new(codes);
}

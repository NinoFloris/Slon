using System;
using Slon.Pg;

namespace Slon.Protocol.Pg;

readonly struct RowRepresentation
{
    RowRepresentation(DataFormat repr) => ForAll = repr;
    RowRepresentation(ReadOnlyMemory<DataFormat> reprs) => PerColumn = reprs;

    public bool IsForAll => PerColumn.IsEmpty;
    public bool IsPerColumn => !IsForAll;

    public DataFormat ForAll { get; }
    public ReadOnlyMemory<DataFormat> PerColumn { get; }

    public static RowRepresentation CreateForAll(DataFormat code) => new(code);
    public static RowRepresentation CreatePerColumn(ReadOnlyMemory<DataFormat> codes) => new(codes);
}

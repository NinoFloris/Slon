using System;
using Npgsql.Pipelines.Pg.Types;

namespace Npgsql.Pipelines.Pg;

// TODO decide whether to keep the two methods merged or split them.
interface IPgConverterInfoResolver
{
    // /// Called when only a DataTypeName is known, this should return the most appropriate/default clr type to convert with.
    // PgConverterInfo? GetDefaultConverterInfo(DataTypeName dataTypeName, PgConverterOptions options);

    PgConverterInfo? GetConverterInfo(Type? type, DataTypeName? dataTypeName, PgConverterOptions options);
}

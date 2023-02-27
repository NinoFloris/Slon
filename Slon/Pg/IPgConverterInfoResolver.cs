using System;
using System.Diagnostics.CodeAnalysis;
using Slon.Pg.Types;

namespace Slon.Pg;

// TODO decide whether to keep the two methods merged or split them.
interface IPgConverterInfoResolver
{
    // /// Called when only a DataTypeName is known, this should return the most appropriate/default clr type to convert with.
    // PgConverterInfo? GetDefaultConverterInfo(DataTypeName dataTypeName, PgConverterOptions options);

    [RequiresUnreferencedCode("Reflection used for pg type conversions.")]
    PgConverterInfo? GetConverterInfo(Type? type, DataTypeName? dataTypeName, PgConverterOptions options);
}

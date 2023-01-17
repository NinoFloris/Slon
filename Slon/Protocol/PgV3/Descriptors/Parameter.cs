using Slon.Pg.Types;

namespace Slon.Protocol.PgV3.Descriptors;

/// A descriptive record on a parameter appearing in a statement text.
/// See ParameterDescription in https://www.postgresql.org/docs/current/static/protocol-message-formats.html
readonly record struct StatementParameter(Oid Oid)
{
    // See https://github.com/postgres/postgres/blob/a7192326c74da417d024a189da4d33c1bf1b40b6/src/interfaces/libpq/libpq-fe.h#L441
    /// The maximum amount of parameters that can be sent or returned.
    public const int MaxCount = ushort.MaxValue;
}

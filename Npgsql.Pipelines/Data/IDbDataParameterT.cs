using System.Data;

namespace Npgsql.Pipelines.Data;

interface IDbDataParameter<T>: IDbDataParameter
{
    new T? Value { get; set; }
}

using System.Data;

namespace Slon.Data;

interface IDbDataParameter<T>: IDbDataParameter
{
    new T? Value { get; set; }
}

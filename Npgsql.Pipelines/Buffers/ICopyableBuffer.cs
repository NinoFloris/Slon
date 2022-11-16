using System.Buffers;

namespace Npgsql.Pipelines.Buffers;

interface ICopyableBuffer<T>
{
    void CopyTo<TWriter>(TWriter destination) where TWriter: IBufferWriter<T>;
}

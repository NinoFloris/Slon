using System.Buffers;

namespace Slon.Buffers;

interface ICopyableBuffer<T>
{
    void CopyTo<TWriter>(TWriter destination) where TWriter: IBufferWriter<T>;
}

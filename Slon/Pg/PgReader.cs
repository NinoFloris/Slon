using System;
using System.Buffers;
using System.Threading.Tasks;

namespace Slon.Pg;

// TODO ArraySequenceReader
class PgReader
{
    public int ByteCount { get; internal set; }
    public DataRepresentation DataRepresentation { get; internal set; }

    public byte ReadByte()
    {
        throw new NotImplementedException();
    }

    public short ReadInt16()
    {
        throw new NotImplementedException();
    }

    public int ReadInt32()
    {
        throw new NotImplementedException();
    }

    public long ReadInt64()
    {
        throw new NotImplementedException();
    }

    public ReadOnlySequence<byte> ReadExact(int byteCount)
    {
        throw new NotImplementedException();
    }
}

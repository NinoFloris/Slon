using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg;

// TODO ArraySequenceReader
class PgReader
{
    public int ByteCount { get; internal set; }
    public DataFormat Format { get; internal set; }
    public int Remaining { get; }

    public void WaitForData(int count)
    {
        
    }
    
    public ValueTask WaitForDataAsync(int count, CancellationToken cancellationToken = default)
    {
        return new();
    }

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

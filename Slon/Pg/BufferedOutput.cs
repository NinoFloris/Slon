using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg;

readonly struct BufferedOutput: IDisposable
{
    readonly ReadOnlySequence<byte> _sequence;

    public BufferedOutput(ReadOnlySequence<byte> sequence)
    {
        _sequence = sequence;
        Length = (int)_sequence.Length;
    }

    public int Length { get; }

    public void Write(PgWriter writer) => writer.WriteRaw(_sequence);
    public ValueTask WriteAsync(PgWriter writer, CancellationToken cancellationToken) => writer.WriteRawAsync(_sequence, cancellationToken);

    // TODO
    public void Dispose()
    {
        var position = default(SequencePosition);
        while (_sequence.TryGet(ref position, out _))
        {
            var obj = position.GetObject();
        }
    }
}

namespace System.Security.Cryptography;

static class HashAlgorithmExtensions
{
    public static bool TryComputeHash(this HashAlgorithm algo, ReadOnlySpan<byte> source, Span<byte> destination, out int bytesWritten)
    {
#if NETSTANDARD2_0
        var dest = algo.ComputeHash(source.ToArray());
        if (dest.Length > destination.Length)
        {
            bytesWritten = 0;
            return false;
        }
        dest.CopyTo(destination);
        bytesWritten = dest.Length;
        return true;
#else
        return algo.TryComputeHash(source, destination, out bytesWritten);
#endif
    }
}

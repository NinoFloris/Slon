using System;
using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using Slon.Protocol.Pg;

namespace Slon.Protocol.PgV3;

readonly struct PasswordMessage : IFrontendMessage
{
    // Technically username and password should be a bucket of bytes, unix style, but we can't really sell that as an api.
    static Encoding Encoding => PgOptions.PasswordEncoding;

    readonly SizedString _hashedPassword;

    public PasswordMessage(string username, string plainPassword, ReadOnlyMemory<byte> salt)
    {
        _hashedPassword = ((SizedString)HashPassword(username, plainPassword, salt.Span, Encoding)).WithEncoding(Encoding);
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.Create(FrontendCode.Password, MessageWriter.GetCStringByteCount(_hashedPassword, Encoding)).Write(ref buffer);
        buffer.WriteCString(_hashedPassword, Encoding);
    }

    internal static string HashPassword(string username, string plainPassword, ReadOnlySpan<byte> salt, Encoding encoding)
    {
        if (plainPassword is null || salt.Length != 4)
            throw new Exception();

        var plaintext = ArrayPool<byte>.Shared.Rent(encoding.GetByteCount(plainPassword) + encoding.GetByteCount(username));
        var passwordEncodedCount = encoding.GetBytes(plainPassword.AsSpan(), plaintext);
        var usernameEncodedCount = encoding.GetBytes(username.AsSpan(), plaintext.AsSpan(passwordEncodedCount));

        using var md5 = MD5.Create();
        var hashSize = md5.HashSize / 8;

        var pgHash = ArrayPool<byte>.Shared.Rent(hashSize);
        if (!md5.TryComputeHash(plaintext.AsSpan(0, passwordEncodedCount + usernameEncodedCount), pgHash, out _))
            ThrowInvalidLength();
        ArrayPool<byte>.Shared.Return(plaintext, clearArray: true);
        var pgHexHash = ConvertShim.ToHexString(pgHash).ToLowerInvariant();

        var plainChallenge = ArrayPool<byte>.Shared.Rent(encoding.GetByteCount(pgHexHash) + salt.Length);
        var hexHashEncodedCount = encoding.GetBytes(pgHexHash.AsSpan(), plainChallenge);
        salt.CopyTo(plainChallenge.AsSpan(hexHashEncodedCount));
        // We reuse pghash as the final output given md5 is always the same size.
        var challengeHash = pgHash;
        if (!md5.TryComputeHash(plainChallenge.AsSpan(0, hexHashEncodedCount + salt.Length), challengeHash, out _))
            ThrowInvalidLength();
        ArrayPool<byte>.Shared.Return(plainChallenge, clearArray: true);

        var result = string.Concat("md5", ConvertShim.ToHexString(challengeHash).ToLowerInvariant());
        ArrayPool<byte>.Shared.Return(challengeHash, clearArray: true);
        return result;

        static void ThrowInvalidLength() => throw new InvalidOperationException("Dev error, md5 is not a variable size algo.");
    }
}

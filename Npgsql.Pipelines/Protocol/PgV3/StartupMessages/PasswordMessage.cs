using System;
using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using Npgsql.Pipelines.Protocol.Pg;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct PasswordMessage : IFrontendMessage
{
    readonly Encoding _encoding;
    readonly SizedString _hashedPassword;

    public PasswordMessage(string username, string plainPassword, ReadOnlyMemory<byte> salt, Encoding encoding)
    {
        _encoding = encoding;
        _hashedPassword = ((SizedString)HashPassword(username, plainPassword, salt.Span)).WithEncoding(encoding);
    }

    public bool CanWrite => true;
    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        PgV3FrontendHeader.Create(FrontendCode.Password, MessageWriter.GetCStringByteCount(_hashedPassword, _encoding)).Write(ref buffer);
        buffer.WriteCString(_hashedPassword, _encoding);
    }

    // Technically username and password should be a bucket of bytes, unix style, but we can't really sell that as an api.
    Encoding Encoding => PgOptions.PasswordEncoding;

    string HashPassword(string username, string plainPassword, ReadOnlySpan<byte> salt)
    {
        if (plainPassword is null || salt.Length != 4)
            throw new Exception();

        var plaintext = ArrayPool<byte>.Shared.Rent(Encoding.GetByteCount(plainPassword) + Encoding.GetByteCount(username));
        var passwordEncodedCount = Encoding.GetBytes(plainPassword.AsSpan(), plaintext);
        Encoding.GetBytes(username.AsSpan(), plaintext.AsSpan(passwordEncodedCount));

        using var md5 = MD5.Create();
        var hashSize = md5.HashSize / 8;

        var pgHash = ArrayPool<byte>.Shared.Rent(hashSize);
        if (!md5.TryComputeHash(plaintext, pgHash, out _))
            ThrowInvalidLength();
        ArrayPool<byte>.Shared.Return(plaintext, clearArray: true);
        var pgHexHash = ConvertShim.ToHexString(pgHash).ToLowerInvariant();

        var plainChallenge = ArrayPool<byte>.Shared.Rent(Encoding.GetByteCount(pgHexHash) + salt.Length);
        var hexHashEncodedCount = Encoding.GetBytes(pgHexHash.AsSpan(), plainChallenge);
        salt.CopyTo(plainChallenge.AsSpan(hexHashEncodedCount));
        // We reuse pghash as the final output given md5 is always the same size.
        var challengeHash = pgHash;
        if (!md5.TryComputeHash(plainChallenge, challengeHash, out _))
            ThrowInvalidLength();
        ArrayPool<byte>.Shared.Return(plainChallenge, clearArray: true);

        var result = string.Concat("md5", ConvertShim.ToHexString(challengeHash).ToLowerInvariant());
        ArrayPool<byte>.Shared.Return(challengeHash, clearArray: true);
        return result;

        static void ThrowInvalidLength() => throw new InvalidOperationException("Dev error, md5 is not a variable size algo.");
    }
}

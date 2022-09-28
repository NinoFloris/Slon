using System;
using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace Npgsql.Pipelines.StartupMessages;

class PasswordMessage : IFrontendMessage
{
    readonly string _hashedPassword;

    public PasswordMessage(string username, string plainPassword, ReadOnlySpan<byte> salt)
    {
        _hashedPassword = HashPassword(username, plainPassword, salt);
    }

    public FrontendCode FrontendCode => FrontendCode.Password;

    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        buffer.WriteCString(_hashedPassword);
    }

    public bool TryPrecomputeLength(out int length)
    {
        length = MessageWriter.GetCStringByteCount(_hashedPassword);
        return true;
    }

    string HashPassword(string username, string plainPassword, ReadOnlySpan<byte> salt)
    {
        if (plainPassword is null || salt.Length != 4)
            throw new Exception();

        var plaintextBuf = ArrayPool<byte>.Shared.Rent(PgEncoding.UTF8.GetByteCount(plainPassword) + PgEncoding.UTF8.GetByteCount(username));
        var passwordByteSize = PgEncoding.UTF8.GetBytes(plainPassword.AsSpan(), plaintextBuf);
        PgEncoding.UTF8.GetBytes(plainPassword.AsSpan(), plaintextBuf.AsSpan().Slice(passwordByteSize));

        using var md5 = MD5.Create();
        var hashSize = md5.HashSize / 8;
        var hashBuf = ArrayPool<byte>.Shared.Rent(hashSize);
        if (!md5.TryComputeHash(plaintextBuf, hashBuf, out _))
            throw new InvalidOperationException("Dev error, md5 is not a variable size algo.");
        ArrayPool<byte>.Shared.Return(plaintextBuf, clearArray: true);

        var hexHash = ConvertShim.ToHexString(hashBuf).ToLowerInvariant();
        var hashAndSalt = ArrayPool<byte>.Shared.Rent(PgEncoding.UTF8.GetByteCount(hexHash) + salt.Length);
        var hexHashSize = PgEncoding.UTF8.GetBytes(hexHash.AsSpan(), hashAndSalt);
        salt.CopyTo(hashAndSalt.AsSpan().Slice(hexHashSize));
        if (!md5.TryComputeHash(hashAndSalt, hashBuf, out _))
            throw new InvalidOperationException("Dev error, md5 is not a variable size algo.");
        ArrayPool<byte>.Shared.Return(hashAndSalt, clearArray: true);

        var sb = new StringBuilder("md5");
        sb.Append(ConvertShim.ToHexString(hashBuf).ToLowerInvariant());
        ArrayPool<byte>.Shared.Return(hashBuf);
        return sb.ToString();
    }
}

using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace Npgsql.Pipelines.Protocol.PgV3;

enum AuthenticationType
{
    Ok = 0,
    KerberosV5 = 2,
    CleartextPassword = 3,
    MD5Password = 5,
    SCMCredential = 6,
    GSS = 7,
    GSSContinue = 8,
    SSPI = 9,
    SASL = 10,
    SASLContinue = 11,
    SASLFinal = 12
}

struct AuthenticationRequest : IPgV3BackendMessage, IDisposable
{
    public AuthenticationType AuthenticationType { get; private set; }

    public ReadOnlyMemory<byte> MD5Salt { get; private set; }
    public ReadOnlyMemory<byte> GSSAPIData { get; private set; }
    public ReadOnlyMemory<byte> SASLData { get; private set; }

    public ReadStatus Read(ref MessageReader<PgV3Header> reader)
    {
        if (!reader.MoveNextAndIsExpected(BackendCode.AuthenticationRequest, out var status, ensureBuffered: true))
            return status;

        var _ = reader.TryReadInt(out var rq);
        AuthenticationType = (AuthenticationType)rq;
        if (BackendMessage.DebugEnabled && !EnumShim.IsDefined(AuthenticationType))
            throw new Exception("Unknown authentication request type code: " + AuthenticationType);

        switch (AuthenticationType)
        {
            case AuthenticationType.MD5Password:
                var salt = ArrayPool<byte>.Shared.Rent(4);
                if(!reader.TryCopyTo(salt.AsSpan(0, 4)))
                    return ReadStatus.InvalidData;
                reader.Advance(4);
                MD5Salt = salt;
                break;
        }

        return ReadStatus.Done;
    }

    public void Dispose()
    {
        if (MemoryMarshal.TryGetArray(MD5Salt, out var md5))
            ArrayPool<byte>.Shared.Return(md5.Array!);

        if (MemoryMarshal.TryGetArray(GSSAPIData, out var gss))
            ArrayPool<byte>.Shared.Return(gss.Array!);

        if (MemoryMarshal.TryGetArray(SASLData, out var sasl))
            ArrayPool<byte>.Shared.Return(sasl.Array!);
    }
}

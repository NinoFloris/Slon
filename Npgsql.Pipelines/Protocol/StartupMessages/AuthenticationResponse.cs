using System;
using System.Buffers;

namespace Npgsql.Pipelines.StartupMessages;

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

class AuthenticationResponse : IBackendMessage
{
    public AuthenticationType AuthenticationType { get; private set; }

    public byte[]? MD5Salt { get; private set; }
    public byte[]? GSSAPIData { get; private set; }
    public byte[]? SASLData { get; private set; }

    public ReadStatus Read(ref MessageReader reader)
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
                var salt = new byte[4];
                if(!reader.TryCopyTo(salt))
                    return ReadStatus.InvalidData;
                reader.Advance(4);
                MD5Salt = salt;
                break;
        }

        return ReadStatus.Done;
    }
}

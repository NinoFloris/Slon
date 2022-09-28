namespace Npgsql.Pipelines;

static class BackendMessageDebug {
    public static bool Enabled { get; } = false;
}

enum BackendCode : byte
{
    AuthenticationRequest   = (byte)'R',
    BackendKeyData          = (byte)'K',
    BindComplete            = (byte)'2',
    CloseComplete           = (byte)'3',
    CommandComplete         = (byte)'C',
    CopyData                = (byte)'d',
    CopyDone                = (byte)'c',
    CopyBothResponse        = (byte)'W',
    CopyInResponse          = (byte)'G',
    CopyOutResponse         = (byte)'H',
    DataRow                 = (byte)'D',
    EmptyQueryResponse      = (byte)'I',
    ErrorResponse           = (byte)'E',
    FunctionCall            = (byte)'F',
    FunctionCallResponse    = (byte)'V',
    NoData                  = (byte)'n',
    NoticeResponse          = (byte)'N',
    NotificationResponse    = (byte)'A',
    ParameterDescription    = (byte)'t',
    ParameterStatus         = (byte)'S',
    ParseComplete           = (byte)'1',
    PasswordPacket          = (byte)' ',
    PortalSuspended         = (byte)'s',
    ReadyForQuery           = (byte)'Z',
    RowDescription          = (byte)'T',
}

public enum ReadStatus
{
    Done,
    NeedMoreData,
    InvalidData, // Desync
    AsyncResponse
}

interface IBackendMessage
{
    ReadStatus Read(ref MessageReader reader);
}

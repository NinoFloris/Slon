namespace Slon.Pg;

enum SizeResultKind: byte
{
    Size,
    FixedSize,
    UpperBound,
    Unknown
}

readonly record struct SizeResult
{
    readonly int _byteCount;

    SizeResult(int byteCount, SizeResultKind kind)
    {
        _byteCount = byteCount;
        Kind = kind;
    }

    public int? Value
    {
        get
        {
            if (Kind is SizeResultKind.Unknown)
                return null;

            return _byteCount;
        }
    }
    public SizeResultKind Kind { get; }

    public static SizeResult Create(int byteCount) => new(byteCount, SizeResultKind.Size);
    public static SizeResult Create(int byteCount, bool fixedSize) => new(byteCount, fixedSize ? SizeResultKind.FixedSize : SizeResultKind.Size);
    public static SizeResult CreateUpperBound(int byteCount) => new(byteCount, SizeResultKind.UpperBound);
    public static SizeResult Unknown => new(default, SizeResultKind.Unknown);
    public static SizeResult Zero => new(0, SizeResultKind.Size);

    public SizeResult Combine(SizeResult result)
    {
        if (Kind is SizeResultKind.Unknown || result.Kind is SizeResultKind.Unknown)
            return this;

        if (Kind is SizeResultKind.UpperBound || result.Kind is SizeResultKind.UpperBound)
            return CreateUpperBound(_byteCount + result._byteCount);

        if (Kind is SizeResultKind.Size || result.Kind is SizeResultKind.Size)
            return Create(_byteCount + result._byteCount);

        return Create(_byteCount + result._byteCount, fixedSize: true);
    }
}

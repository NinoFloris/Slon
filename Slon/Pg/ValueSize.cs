namespace Slon.Pg;

enum ValueSizeKind: byte
{
    Size,
    UpperBound,
    Unknown
}

readonly record struct ValueSize
{
    readonly int _byteCount;

    ValueSize(int byteCount, ValueSizeKind kind)
    {
        _byteCount = byteCount;
        Kind = kind;
    }

    public int? Value
    {
        get
        {
            if (Kind is ValueSizeKind.Unknown)
                return null;

            return _byteCount;
        }
    }
    public ValueSizeKind Kind { get; }

    public static ValueSize Create(int byteCount) => new(byteCount, ValueSizeKind.Size);
    public static ValueSize CreateUpperBound(int byteCount) => new(byteCount, ValueSizeKind.UpperBound);
    public static ValueSize Unknown => new(default, ValueSizeKind.Unknown);
    public static ValueSize Zero => new(0, ValueSizeKind.Size);

    public ValueSize Combine(ValueSize result)
    {
        if (Kind is ValueSizeKind.Unknown || result.Kind is ValueSizeKind.Unknown)
            return this;

        if (Kind is ValueSizeKind.UpperBound || result.Kind is ValueSizeKind.UpperBound)
            return CreateUpperBound(_byteCount + result._byteCount);

        return Create(_byteCount + result._byteCount);
    }

    public static implicit operator ValueSize(int value) => Create(value);
}

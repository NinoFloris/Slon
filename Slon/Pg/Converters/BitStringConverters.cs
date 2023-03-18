using System;
using System.Buffers;
using System.Collections;
using System.Collections.Specialized;
using Slon.Pg.Descriptors;
using Slon.Pg.Types;

namespace Slon.Pg.Converters;

sealed class BitArrayBitStringConverter : PgConverter<BitArray>
{
    readonly ArrayPool<byte> _arrayPool;
    public BitArrayBitStringConverter(PgConverterOptions options) => _arrayPool = options.GetArrayPool<byte>();

    public static BitArray ReadValue(PgReader reader)
        => new(reader.ReadExact((reader.ReadInt32() + 7) / 8).ToArray());

    public override BitArray Read(PgReader reader) => ReadValue(reader);
    public override ValueSize GetSize(SizeContext context, BitArray value, ref object? writeState)
        => sizeof(int) + (value.Length + 7) / 8;

    public override void Write(PgWriter writer, BitArray value)
    {
        var array = _arrayPool.Rent((value.Length + 7) / 8);
        value.CopyTo(array, 0);

        writer.WriteInt32(value.Length);
        writer.WriteRaw(new ReadOnlySequence<byte>(array));

        _arrayPool.Return(array);
    }
}

sealed class BitVector32BitStringConverter : PgConverter<BitVector32>
{
    public override BitVector32 Read(PgReader reader)
    {
        if (reader.ByteCount > sizeof(int) + sizeof(int))
            throw new InvalidCastException("Can't read a BIT(N) with more than 32 bits to BitVector32, only up to BIT(32).");

        return new(reader.ReadInt32() is 0 ? 0 : reader.ReadInt32());
    }

    public override ValueSize GetSize(SizeContext context, BitVector32 value, ref object? writeState)
        => value.Data is 0 ? 4 : 8;

    public override void Write(PgWriter writer, BitVector32 value)
    {
        if (value.Data == 0)
            writer.WriteInt32(0);
        else
        {
            writer.WriteInt32(32);
            writer.WriteInt32(value.Data);
        }
    }
}

sealed class BoolBitStringConverter : PgFixedBinarySizeConverter<bool>
{
    public static bool ReadValue(PgReader reader)
    {
        if (reader.ReadInt32() > 1)
            throw new InvalidCastException("Can't read a BIT(N) type to bool, only BIT(1).");

        return (reader.ReadByte() & 128) is not 0;
    }

    protected override byte BinarySize => 5;
    protected override bool ReadCore(PgReader reader) => ReadValue(reader);
    public override void Write(PgWriter writer, bool value)
    {
        writer.WriteInt32(1);
        writer.WriteByte(value ? (byte)128 : (byte)0);
    }
}

/// Note that for BIT(1), this resolver will return a bool by default, to align with SQLClient
/// (see discussion https://github.com/npgsql/npgsql/pull/362#issuecomment-59622101).
sealed class PolymorphicBitStringConverterResolver : PolymorphicReadConverterResolver
{
    PolymorphicReadConverter? _bitArrayConverter;
    PolymorphicReadConverter? _boolConverter;

    public PolymorphicBitStringConverterResolver(PgTypeId bitString) : base(bitString) {}

    protected override PolymorphicReadConverter Get(Field? field)
        => field?.TypeModifier is 1
            ? _boolConverter ??= new PolymorphicBitStringConverter<bool>()
            : _bitArrayConverter ??= new PolymorphicBitStringConverter<BitArray>();

    sealed class PolymorphicBitStringConverter<TEffective> : PolymorphicReadConverter
    {
        public PolymorphicBitStringConverter() : base(typeof(TEffective)) { }

        public override object Read(PgReader reader)
            => typeof(TEffective) == typeof(bool)
                ? BoolBitStringConverter.ReadValue(reader)
                : BitArrayBitStringConverter.ReadValue(reader);
    }
}

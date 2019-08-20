#include <Storages/Transaction/Codec.h>

#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiKVVarInt.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

constexpr int digitsPerWord = 9;
constexpr int wordSize = 4;
const int dig2Bytes[10] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

template <typename B, typename A>
inline B enforce_cast(A a)
{
    if constexpr (std::is_same_v<A, B>)
    {
        return a;
    }
    else if constexpr (sizeof(B) == sizeof(A))
    {
        B b;
        memcpy(&b, &a, sizeof(A));
        return b;
    }
    else
    {
        throw Exception("Cannot cast! (enforce_cast)", ErrorCodes::LOGICAL_ERROR);
    }
}

Float64 DecodeFloat64(size_t & cursor, const String & raw_value)
{
    UInt64 num = DecodeUInt<UInt64>(cursor, raw_value);
    if (num & SIGN_MASK)
        num ^= SIGN_MASK;
    else
        num = ~num;
    return enforce_cast<Float64>(num);
}

template <typename StringStream>
void DecodeBytes(size_t & cursor, const String & raw_value, StringStream & ss)
{
    while (true)
    {
        size_t next_cursor = cursor + 9;
        if (next_cursor > raw_value.size())
            throw Exception("Wrong format, cursor over buffer size. (DecodeBytes)", ErrorCodes::LOGICAL_ERROR);
        UInt8 marker = (UInt8)raw_value[cursor + 8];
        UInt8 pad_size = ENC_MARKER - marker;

        if (pad_size > 8)
            throw Exception("Wrong format, too many padding bytes. (DecodeBytes)", ErrorCodes::LOGICAL_ERROR);
        ss.write(&raw_value[cursor], 8 - pad_size);
        cursor = next_cursor;
        if (pad_size != 0)
            break;
    }
}

String DecodeBytes(size_t & cursor, const String & raw_value)
{
    std::stringstream ss;
    DecodeBytes(cursor, raw_value, ss);
    return ss.str();
}

struct NullStringStream
{
    void write(const char *, size_t) {}
};

void SkipBytes(size_t & cursor, const String & raw_value)
{
    NullStringStream ss;
    DecodeBytes(cursor, raw_value, ss);
}

String DecodeCompactBytes(size_t & cursor, const String & raw_value)
{
    size_t size = DecodeVarInt(cursor, raw_value);
    String res(&raw_value[cursor], size);
    cursor += size;
    return res;
}

void SkipCompactBytes(size_t & cursor, const String & raw_value)
{
    size_t size = DecodeVarInt(cursor, raw_value);
    cursor += size;
}

Int64 DecodeVarInt(size_t & cursor, const String & raw_value)
{
    UInt64 v = DecodeVarUInt(cursor, raw_value);
    Int64 vx = v >> 1;
    return (v & 1) ? ~vx : vx;
}

void SkipVarInt(size_t & cursor, const String & raw_value) { SkipVarUInt(cursor, raw_value); }

UInt64 DecodeVarUInt(size_t & cursor, const String & raw_value)
{
    UInt64 res = 0;
    int s = 0;
    for (int i = 0; cursor < raw_value.size(); i++)
    {
        UInt64 v = raw_value[cursor++];
        if (v < 0x80)
        {
            if (i > 9 || (i == 9 && v > 1))
                throw Exception("Overflow when DecodeVarUInt", ErrorCodes::LOGICAL_ERROR);
            return res | v << s;
        }
        res |= (v & 0x7f) << s;
        s += 7;
    }
    throw Exception("Wrong format. (DecodeVarUInt)", ErrorCodes::LOGICAL_ERROR);
}

void SkipVarUInt(size_t & cursor, const String & raw_value) { std::ignore = DecodeVarUInt(cursor, raw_value); }

inline Int8 getWords(PrecType prec, ScaleType scale)
{
    Int8 scale_word = scale / 9 + (scale % 9 > 0);
    Int8 int_word = (prec - scale) / 9 + ((prec - scale) % 9 > 0);
    return scale_word + int_word;
}

inline int getBytes(PrecType prec, ScaleType scale)
{
    int digitsInt = prec - scale;
    int wordsInt = digitsInt / digitsPerWord;
    int wordsFrac = scale / digitsPerWord;
    int xInt = digitsInt - wordsInt * digitsPerWord; // leading digits.
    int xFrac = scale - wordsFrac * digitsPerWord;   // traling digits.
    return wordsInt * wordSize + dig2Bytes[xInt] + wordsFrac * wordSize + dig2Bytes[xFrac];
}

inline UInt32 readWord(int binIdx, const String & dec, int size)
{
    UInt32 v = 0;
    switch (size)
    {
        case 1:
            v = Int32(Int8(dec[binIdx]));
            break;
        case 2:
            if ((dec[binIdx] & 128) > 0)
                v = (255 << 24) | (255 << 16) | (UInt8(dec[binIdx]) << 8) | UInt8(dec[binIdx + 1]);
            else
                v = (UInt8(dec[binIdx]) << 8) | UInt8(dec[binIdx + 1]);
            break;
        case 3:
            if ((dec[binIdx] & 128) > 0)
            {
                v = (255 << 24) | (UInt8(dec[binIdx]) << 16) | (UInt8(dec[binIdx + 1]) << 8) | UInt8(dec[binIdx + 2]);
            }
            else
            {
                v = (UInt8(dec[binIdx]) << 16) | (UInt8(dec[binIdx + 1]) << 8) | UInt8(dec[binIdx + 2]);
            }
            break;
        case 4:
            v = (UInt8(dec[binIdx]) << 24) | (UInt8(dec[binIdx + 1]) << 16) | (UInt8(dec[binIdx + 2]) << 8) | UInt8(dec[binIdx + 3]);
            break;
    }
    return v;
}

Decimal DecodeDecimal(size_t & cursor, const String & raw_value)
{
    PrecType prec = raw_value[cursor++];
    ScaleType frac = raw_value[cursor++];

    int digitsInt = prec - frac;
    int wordsInt = digitsInt / digitsPerWord;
    int leadingDigits = digitsInt - wordsInt * digitsPerWord;
    int wordsFrac = frac / digitsPerWord;
    int trailingDigits = frac - wordsFrac * digitsPerWord;
    //    int wordsIntTo = wordsInt + (leadingDigits > 0);
    //    int wordsFracTo = wordsFrac + (trailingDigits > 0);

    int binSize = getBytes(prec, frac);
    String dec = raw_value.substr(cursor, binSize);
    cursor += binSize;
    int mask = -1;
    int binIdx = 0;
    if (dec[binIdx] & 0x80)
    {
        mask = 0;
    }
    dec[0] ^= 0x80;

    int256_t value = 0;

    if (leadingDigits)
    {
        int i = dig2Bytes[leadingDigits];
        UInt32 x = readWord(binIdx, dec, i);
        binIdx += i;
        value = x ^ mask;
    }
    const int wordMax = int(1e9);
    for (int stop = binIdx + wordsInt * wordSize + wordsFrac * wordSize; binIdx < stop; binIdx += wordSize)
    {
        UInt32 v = readWord(binIdx, dec, 4) ^ mask;
        if (v >= wordMax)
        {
            throw Exception("bad number: " + std::to_string(v));
        }
        value *= wordMax;
        value += v;
    }
    if (trailingDigits)
    {
        int len = dig2Bytes[trailingDigits];
        UInt32 x = readWord(binIdx, dec, len);
        for (int i = 0; i < trailingDigits; i++)
            value *= 10;
        value += x ^ mask;
    }
    if (mask)
        value = -value;
    return Decimal(value, prec, frac);
}

void SkipDecimal(size_t & cursor, const String & raw_value)
{
    PrecType prec = raw_value[cursor++];
    ScaleType frac = raw_value[cursor++];

    int binSize = getBytes(prec, frac);
    cursor += binSize;
}

Field DecodeDatum(size_t & cursor, const String & raw_value)
{
    switch (raw_value[cursor++])
    {
        case TiDB::CodecFlagNil:
            return Field();
        case TiDB::CodecFlagInt:
            return DecodeInt64(cursor, raw_value);
        case TiDB::CodecFlagUInt:
            return DecodeUInt<UInt64>(cursor, raw_value);
        case TiDB::CodecFlagBytes:
            return DecodeBytes(cursor, raw_value);
        case TiDB::CodecFlagCompactBytes:
            return DecodeCompactBytes(cursor, raw_value);
        case TiDB::CodecFlagFloat:
            return DecodeFloat64(cursor, raw_value);
        case TiDB::CodecFlagVarUInt:
            return DecodeVarUInt(cursor, raw_value);
        case TiDB::CodecFlagVarInt:
            return DecodeVarInt(cursor, raw_value);
        case TiDB::CodecFlagDuration:
            throw Exception("Not implented yet. DecodeDatum: CodecFlagDuration", ErrorCodes::LOGICAL_ERROR);
        case TiDB::CodecFlagDecimal:
            return DecodeDecimal(cursor, raw_value);
        default:
            throw Exception("Unknown Type:" + std::to_string(raw_value[cursor - 1]), ErrorCodes::LOGICAL_ERROR);
    }
}

void SkipDatum(size_t & cursor, const String & raw_value)
{
    switch (raw_value[cursor++])
    {
        case TiDB::CodecFlagNil:
            return;
        case TiDB::CodecFlagInt:
            cursor += sizeof(Int64);
            return;
        case TiDB::CodecFlagUInt:
            cursor += sizeof(UInt64);
            return;
        case TiDB::CodecFlagBytes:
            SkipBytes(cursor, raw_value);
            return;
        case TiDB::CodecFlagCompactBytes:
            SkipCompactBytes(cursor, raw_value);
            return;
        case TiDB::CodecFlagFloat:
            cursor += sizeof(UInt64);
            return;
        case TiDB::CodecFlagVarUInt:
            SkipVarUInt(cursor, raw_value);
            return;
        case TiDB::CodecFlagVarInt:
            SkipVarInt(cursor, raw_value);
            return;
        case TiDB::CodecFlagDuration:
            throw Exception("Not implented yet. DecodeDatum: CodecFlagDuration", ErrorCodes::LOGICAL_ERROR);
        case TiDB::CodecFlagDecimal:
            SkipDecimal(cursor, raw_value);
            return;
        default:
            throw Exception("Unknown Type:" + std::to_string(raw_value[cursor - 1]), ErrorCodes::LOGICAL_ERROR);
    }
}

void EncodeFloat64(Float64 num, std::stringstream & ss)
{
    UInt64 u = enforce_cast<UInt64>(num);
    if (u & SIGN_MASK)
        u = ~u;
    else
        u |= SIGN_MASK;
    return EncodeUInt<UInt64>(u, ss);
}

void EncodeBytes(const String & ori_str, std::stringstream & ss)
{
    size_t len = ori_str.size();
    size_t index = 0;
    while (index <= len)
    {
        size_t remain = len - index;
        size_t pad = 0;
        if (remain >= ENC_GROUP_SIZE)
        {
            ss.write(ori_str.data() + index, ENC_GROUP_SIZE);
        }
        else
        {
            pad = ENC_GROUP_SIZE - remain;
            ss.write(ori_str.data() + index, remain);
            ss.write(ENC_ASC_PADDING, pad);
        }
        ss.put(static_cast<char>(ENC_MARKER - (UInt8)pad));
        index += ENC_GROUP_SIZE;
    }
}

void EncodeCompactBytes(const String & str, std::stringstream & ss)
{
    TiKV::writeVarInt(Int64(str.size()), ss);
    ss.write(str.c_str(), str.size());
}

void EncodeVarInt(Int64 num, std::stringstream & ss) { TiKV::writeVarInt(num, ss); }

void EncodeVarUInt(UInt64 num, std::stringstream & ss) { TiKV::writeVarUInt(num, ss); }

void EncodeDecimal(const Decimal &, std::stringstream &) { throw Exception("To be implemented", ErrorCodes::LOGICAL_ERROR); }

template <typename T>
inline T getFieldValue(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::UInt64:
            return static_cast<T>(field.get<UInt64>());
        case Field::Types::Int64:
            return static_cast<T>(field.get<Int64>());
        case Field::Types::Float64:
            return static_cast<T>(field.get<Float64>());
        case Field::Types::Decimal:
            return static_cast<T>(field.get<Decimal>());
        default:
            throw Exception("Unsupport (getFieldValue): " + std::string(field.getTypeName()), ErrorCodes::LOGICAL_ERROR);
    }
}

void EncodeDatum(const Field & field, TiDB::CodecFlag flag, std::stringstream & ss)
{
    ss << UInt8(flag);
    switch (flag)
    {
        case TiDB::CodecFlagDecimal:
            return EncodeDecimal(getFieldValue<Decimal>(field), ss);
        case TiDB::CodecFlagCompactBytes:
            return EncodeCompactBytes(field.get<String>(), ss);
        case TiDB::CodecFlagFloat:
            return EncodeFloat64(getFieldValue<Float64>(field), ss);
        case TiDB::CodecFlagUInt:
            return EncodeUInt<UInt64>(getFieldValue<UInt64>(field), ss);
        case TiDB::CodecFlagInt:
            return EncodeInt64(getFieldValue<Int64>(field), ss);
        case TiDB::CodecFlagVarInt:
            return EncodeVarInt(getFieldValue<Int64>(field), ss);
        case TiDB::CodecFlagVarUInt:
            return EncodeVarUInt(getFieldValue<UInt64>(field), ss);
        default:
            throw Exception("Not implented codec flag: " + std::to_string(flag), ErrorCodes::LOGICAL_ERROR);
    }
}

} // namespace DB

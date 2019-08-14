#include <Storages/Transaction/Codec.h>

#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiKVVarInt.h>
#include <DataTypes/DataTypeDecimal.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

constexpr UInt64 signMask = UInt64(1) << 63;
constexpr UInt32 signMask32 = UInt32(1) << 31;

constexpr int digitsPerWord = 9;
constexpr int wordSize = 4;
constexpr int dig2Bytes[10] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
constexpr int powers10[10] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

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
    UInt64 num = DecodeNumber<UInt64>(cursor, raw_value);
    if (num & signMask)
        num ^= signMask;
    else
        num = ~num;
    return enforce_cast<Float64>(num);
}

String DecodeBytes(size_t & cursor, const String & raw_value)
{
    std::stringstream ss;
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
    return ss.str();
}

String DecodeCompactBytes(size_t & cursor, const String & raw_value)
{
    size_t size = DecodeVarInt(cursor, raw_value);
    String res(&raw_value[cursor], size);
    cursor += size;
    return res;
}

Int64 DecodeVarInt(size_t & cursor, const String & raw_value)
{
    UInt64 v = DecodeVarUInt(cursor, raw_value);
    Int64 vx = v >> 1;
    return (v & 1) ? ~vx : vx;
}

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

template<typename T>
inline T DecodeDecimalImpl(size_t & cursor, const String & raw_value, PrecType prec, ScaleType frac)
{
    static_assert(IsDecimal<T>);

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

    typename T::NativeType value = 0;

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
    return value;
}

Field DecodeDecimal(size_t & cursor, const String & raw_value)
{
    PrecType prec = raw_value[cursor++];
    ScaleType scale = raw_value[cursor++];
    auto type = createDecimal(prec, scale);
    if (checkDecimal<Decimal32>(*type)) {
        auto res = DecodeDecimalImpl<Decimal32>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal32>(res, scale);
    } else if (checkDecimal<Decimal64>(*type)) {
        auto res = DecodeDecimalImpl<Decimal64>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal64>(res, scale);
    } else if (checkDecimal<Decimal128>(*type)) {
        auto res = DecodeDecimalImpl<Decimal128>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal128>(res, scale);
    } else {
        auto res = DecodeDecimalImpl<Decimal256>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal256>(res, scale);
    }
}

Field DecodeDatum(size_t & cursor, const String & raw_value)
{
    switch (raw_value[cursor++])
    {
        case TiDB::CodecFlagNil:
            return Field();
        case TiDB::CodecFlagInt:
            return DecodeNumber<Int64>(cursor, raw_value);
        case TiDB::CodecFlagUInt:
            return DecodeNumber<UInt64>(cursor, raw_value);
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

void EncodeFloat64(Float64 num, std::stringstream & ss)
{
    UInt64 u = enforce_cast<UInt64>(num);
    if (u & signMask)
        u = ~u;
    else
        u |= signMask;
    return EncodeNumber<UInt64>(u, ss);
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

inline void writeWord(String & buf, Int32 word, int size)
{
    switch (size)
    {
        case 1:
            buf.push_back(char(word));
            break;
        case 2:
            buf.push_back(char(word>>8));
            buf.push_back(char(word));
            break;
        case 3:
            buf.push_back(char(word>>16));
            buf.push_back(char(word>>8));
            buf.push_back(char(word));
            break;
        case 4:
            buf.push_back(char(word>>24));
            buf.push_back(char(word>>16));
            buf.push_back(char(word>>8));
            buf.push_back(char(word));
            break;
    }
}

template<typename T>
void EncodeDecimal(const T & dec, PrecType prec, ScaleType frac, std::stringstream & ss)
{
    static_assert(IsDecimal<T>);

    constexpr Int32 decimal_mod = powers10[digitsPerWord];
    EncodeNumber(UInt8(prec), ss);
    EncodeNumber(UInt8(frac), ss);

    int digitsInt = prec - frac;
    int wordsInt = digitsInt / digitsPerWord;
    int leadingDigits = digitsInt - wordsInt * digitsPerWord;
    int wordsFrac = frac / digitsPerWord;
    int trailingDigits = frac - wordsFrac * digitsPerWord;
    int words = getWords(prec, frac);

    Int256 value = dec.value;
    Int32 mask = 0;
    if (value < 0)
    {
        value = -value;
        mask = -1;
    }

    std::vector<Int32> v;

    for (int i = 0; i < words; i ++)
    {
        if (i == 0 && trailingDigits > 0)
        {
            v.push_back(static_cast<Int32>(value % powers10[trailingDigits]));
            value /= powers10[trailingDigits];
        }
        else
        {
            v.push_back(static_cast<Int32>(value % decimal_mod));
            value /= decimal_mod;
        }
    }

    reverse(v.begin(), v.end());

    if (value > 0)
        throw Exception("Value is overflow! (EncodeDecimal)", ErrorCodes::LOGICAL_ERROR);

    String buf;
    for (int i = 0; i < words; i++)
    {
        v[i] ^= mask;
        if (i == 0 && leadingDigits > 0)
        {
            int size = dig2Bytes[leadingDigits];
            writeWord(buf, v[i], size);
        }
        else if (i + 1 == words && trailingDigits > 0)
        {
            int size = dig2Bytes[trailingDigits];
            writeWord(buf, v[i], size);
        }
        else
        {
            writeWord(buf, v[i], 4);
        }
    }
    buf[0] ^= 0x80;
    ss.write(buf.c_str(), buf.size());
}

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
        default:
            throw Exception("Unsupport (getFieldValue): " + std::string(field.getTypeName()), ErrorCodes::LOGICAL_ERROR);
    }
}

void EncodeDatum(const Field & field, TiDB::CodecFlag flag, std::stringstream & ss)
{
    EncodeNumber(UInt8(flag), ss);
    switch (flag)
    {
        case TiDB::CodecFlagDecimal:
            if (field.getType() == Field::Types::Decimal32)
            {
                auto decimal_field = field.get<DecimalField<Decimal32>>();
                return EncodeDecimal(decimal_field.getValue(),decimal_field.getPrec(),decimal_field.getScale(), ss);
            }
            else if (field.getType() == Field::Types::Decimal64)
            {
                auto decimal_field = field.get<DecimalField<Decimal64>>();
                return EncodeDecimal(decimal_field.getValue(),decimal_field.getPrec(),decimal_field.getScale(), ss);
            }
            else if (field.getType() == Field::Types::Decimal128)
            {
                auto decimal_field = field.get<DecimalField<Decimal128>>();
                return EncodeDecimal(decimal_field.getValue(),decimal_field.getPrec(),decimal_field.getScale(), ss);
            }
            else
            {
                auto decimal_field = field.get<DecimalField<Decimal256>>();
                return EncodeDecimal(decimal_field.getValue(),decimal_field.getPrec(),decimal_field.getScale(), ss);
            }
        case TiDB::CodecFlagCompactBytes:
            return EncodeCompactBytes(field.get<String>(), ss);
        case TiDB::CodecFlagFloat:
            return EncodeFloat64(getFieldValue<Float64>(field), ss);
        case TiDB::CodecFlagUInt:
            return EncodeNumber<UInt64>(getFieldValue<UInt64>(field), ss);
        case TiDB::CodecFlagInt:
            return EncodeNumber<Int64>(getFieldValue<Int64>(field), ss);
        case TiDB::CodecFlagVarInt:
            return EncodeVarInt(getFieldValue<Int64>(field), ss);
        case TiDB::CodecFlagVarUInt:
            return EncodeVarUInt(getFieldValue<UInt64>(field), ss);
        default:
            throw Exception("Not implented codec flag: " + std::to_string(flag), ErrorCodes::LOGICAL_ERROR);
    }
}

template void EncodeDecimal<Decimal32> (const Decimal32 & , PrecType, ScaleType, std::stringstream & ss);
template void EncodeDecimal<Decimal64> (const Decimal64 & , PrecType, ScaleType, std::stringstream & ss);
template void EncodeDecimal<Decimal128> (const Decimal128 & , PrecType, ScaleType, std::stringstream & ss);
template void EncodeDecimal<Decimal256> (const Decimal256 & , PrecType, ScaleType, std::stringstream & ss);

} // namespace DB

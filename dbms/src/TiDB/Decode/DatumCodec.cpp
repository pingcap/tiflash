// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <DataTypes/DataTypeDecimal.h>
#include <IO/Endian.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Storages/KVStore/TiKVHelpers/TiKVVarInt.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/Decode/JsonBinary.h>
#include <TiDB/Decode/Vector.h>
#include <TiDB/Schema/TiDB.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

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
        auto marker = static_cast<UInt8>(raw_value[cursor + 8]);
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
    WriteBufferFromOwnString ss;
    DecodeBytes(cursor, raw_value, ss);
    return ss.releaseStr();
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

UInt64 DecodeVarUIntImpl(size_t & cursor, const char * raw_value, size_t length)
{
    UInt64 res = 0;
    int s = 0;
    for (int i = 0; cursor < length; i++)
    {
        UInt64 v = static_cast<UInt8>(raw_value[cursor++]);
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
UInt64 DecodeVarUInt(size_t & cursor, const StringRef & raw_value)
{
    return DecodeVarUIntImpl(cursor, raw_value.data, raw_value.size);
}

UInt64 DecodeVarUInt(size_t & cursor, const String & raw_value)
{
    return DecodeVarUIntImpl(cursor, raw_value.data(), raw_value.length());
}

void SkipVarUInt(size_t & cursor, const String & raw_value)
{
    std::ignore = DecodeVarUInt(cursor, raw_value);
}

Int64 DecodeVarInt(size_t & cursor, const String & raw_value)
{
    UInt64 v = DecodeVarUInt(cursor, raw_value);
    Int64 vx = v >> 1;
    return (v & 1) ? ~vx : vx;
}

void SkipVarInt(size_t & cursor, const String & raw_value)
{
    SkipVarUInt(cursor, raw_value);
}

inline Int32 getWords(PrecType prec, ScaleType scale)
{
    Int32 scale_word = scale / 9 + (scale % 9 > 0);
    Int32 int_word = (prec - scale) / 9 + ((prec - scale) % 9 > 0);
    return scale_word + int_word;
}

inline int getBytes(PrecType prec, ScaleType scale)
{
    int digits_int = prec - scale;
    int words_int = digits_int / digitsPerWord;
    int words_frac = scale / digitsPerWord;
    int x_int = digits_int - words_int * digitsPerWord; // leading digits.
    int x_frac = scale - words_frac * digitsPerWord; // traling digits.
    return words_int * wordSize + dig2Bytes[x_int] + words_frac * wordSize + dig2Bytes[x_frac];
}

inline UInt32 readWord(int binIdx, const String & dec, int size)
{
    UInt32 v = 0;
    switch (size)
    {
    case 1:
        v = static_cast<Int32>(static_cast<Int8>(dec[binIdx]));
        break;
    case 2:
        if ((dec[binIdx] & 128) > 0)
            v = (255 << 24) | (255 << 16) | (static_cast<UInt8>(dec[binIdx]) << 8)
                | static_cast<UInt8>(dec[binIdx + 1]);
        else
            v = (static_cast<UInt8>(dec[binIdx]) << 8) | static_cast<UInt8>(dec[binIdx + 1]);
        break;
    case 3:
        if ((dec[binIdx] & 128) > 0)
        {
            v = (255 << 24) | (static_cast<UInt8>(dec[binIdx]) << 16) | (static_cast<UInt8>(dec[binIdx + 1]) << 8)
                | static_cast<UInt8>(dec[binIdx + 2]);
        }
        else
        {
            v = (static_cast<UInt8>(dec[binIdx]) << 16) | (static_cast<UInt8>(dec[binIdx + 1]) << 8)
                | static_cast<UInt8>(dec[binIdx + 2]);
        }
        break;
    case 4:
        v = (static_cast<UInt8>(dec[binIdx]) << 24) | (static_cast<UInt8>(dec[binIdx + 1]) << 16)
            | (static_cast<UInt8>(dec[binIdx + 2]) << 8) | static_cast<UInt8>(dec[binIdx + 3]);
        break;
    }
    return v;
}

template <typename T>
T DecodeDecimalImpl(size_t & cursor, const String & raw_value, PrecType prec, ScaleType frac)
{
    static_assert(IsDecimal<T>);

    int digits_int = prec - frac;
    int words_int = digits_int / digitsPerWord;
    int leading_digits = digits_int - words_int * digitsPerWord;
    int words_frac = frac / digitsPerWord;
    int trailing_digits = frac - words_frac * digitsPerWord;
    //    int wordsIntTo = wordsInt + (leadingDigits > 0);
    //    int wordsFracTo = wordsFrac + (trailingDigits > 0);

    int bin_size = getBytes(prec, frac);
    String dec = raw_value.substr(cursor, bin_size);
    cursor += bin_size;
    int mask = -1;
    int bin_idx = 0;
    if (dec[bin_idx] & 0x80)
    {
        mask = 0;
    }
    dec[0] ^= 0x80;

    typename T::NativeType value = 0;

    if (leading_digits)
    {
        int i = dig2Bytes[leading_digits];
        UInt32 x = readWord(bin_idx, dec, i);
        bin_idx += i;
        value = x ^ mask;
    }
    const int word_max = static_cast<int>(1e9);
    for (int stop = bin_idx + words_int * wordSize + words_frac * wordSize; bin_idx < stop; bin_idx += wordSize)
    {
        UInt32 v = readWord(bin_idx, dec, 4) ^ mask;
        if (v >= word_max)
        {
            throw Exception("bad number: " + std::to_string(v));
        }
        value *= word_max;
        value += v;
    }
    if (trailing_digits)
    {
        int len = dig2Bytes[trailing_digits];
        UInt32 x = readWord(bin_idx, dec, len);
        for (int i = 0; i < trailing_digits; i++)
            value *= 10;
        value += x ^ mask;
    }
    if (mask)
        value = -value;
    return value;
}

Field DecodeDecimalForCHRow(size_t & cursor, const String & raw_value, const TiDB::ColumnInfo & column_info)
{
    PrecType prec = static_cast<UInt8>(raw_value[cursor++]);
    ScaleType scale = static_cast<UInt8>(raw_value[cursor++]);
    auto type = createDecimal(column_info.flen, column_info.decimal);
    if (checkDecimal<Decimal32>(*type))
    {
        auto res = DecodeDecimalImpl<Decimal32>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal32>(res, scale);
    }
    else if (checkDecimal<Decimal64>(*type))
    {
        auto res = DecodeDecimalImpl<Decimal64>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal64>(res, scale);
    }
    else if (checkDecimal<Decimal128>(*type))
    {
        auto res = DecodeDecimalImpl<Decimal128>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal128>(res, scale);
    }
    else
    {
        auto res = DecodeDecimalImpl<Decimal256>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal256>(res, scale);
    }
}

Field DecodeDecimal(size_t & cursor, const String & raw_value)
{
    PrecType prec = static_cast<UInt8>(raw_value[cursor++]);
    ScaleType scale = static_cast<UInt8>(raw_value[cursor++]);
    auto type = createDecimal(prec, scale);
    if (checkDecimal<Decimal32>(*type))
    {
        auto res = DecodeDecimalImpl<Decimal32>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal32>(res, scale);
    }
    else if (checkDecimal<Decimal64>(*type))
    {
        auto res = DecodeDecimalImpl<Decimal64>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal64>(res, scale);
    }
    else if (checkDecimal<Decimal128>(*type))
    {
        auto res = DecodeDecimalImpl<Decimal128>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal128>(res, scale);
    }
    else
    {
        auto res = DecodeDecimalImpl<Decimal256>(cursor, raw_value, prec, scale);
        return DecimalField<Decimal256>(res, scale);
    }
}

void SkipDecimal(size_t & cursor, const String & raw_value)
{
    PrecType prec = static_cast<UInt8>(raw_value[cursor++]);
    ScaleType frac = static_cast<UInt8>(raw_value[cursor++]);

    int bin_size = getBytes(prec, frac);
    cursor += bin_size;
}

Field DecodeDatumForCHRow(size_t & cursor, const String & raw_value, const TiDB::ColumnInfo & column_info)
{
    if (raw_value[cursor] == TiDB::CodecFlagDecimal)
    {
        cursor++;
        return DecodeDecimalForCHRow(cursor, raw_value, column_info);
    }
    else
    {
        return DecodeDatum(cursor, raw_value);
    }
}

void EncodeVectorFloat32(const Array & val, WriteBuffer & ss)
{
    RUNTIME_CHECK(boost::endian::order::native == boost::endian::order::little);

    writeIntBinary(static_cast<UInt32>(val.size()), ss);
    for (const auto & s : val)
        writeFloatBinary(static_cast<Float32>(s.safeGet<NearestFieldType<Float32>::Type>()), ss);
}

void SkipVectorFloat32(size_t & cursor, const String & raw_value)
{
    RUNTIME_CHECK(boost::endian::order::native == boost::endian::order::little);

    auto elements_n = readLittleEndian<UInt32>(&raw_value[cursor]);
    auto size = sizeof(elements_n) + elements_n * sizeof(Float32);
    cursor += size;
}

Field DecodeVectorFloat32(size_t & cursor, const String & raw_value)
{
    RUNTIME_CHECK(boost::endian::order::native == boost::endian::order::little);

    auto n = readLittleEndian<UInt32>(&raw_value[cursor]);
    cursor += sizeof(UInt32);

    Array res;
    res.reserve(n);

    for (size_t i = 0; i < n; i++)
    {
        auto v = readLittleEndian<Float32>(&raw_value[cursor]);
        res.emplace_back(static_cast<Float64>(v));
        cursor += sizeof(Float32);
    }

    return res;
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
        return DecodeInt64(cursor, raw_value);
    case TiDB::CodecFlagDecimal:
        return DecodeDecimal(cursor, raw_value);
    case TiDB::CodecFlagJson:
        return JsonBinary::DecodeJsonAsBinary(cursor, raw_value);
    case TiDB::CodecFlagVectorFloat32:
        return DecodeVectorFloat32(cursor, raw_value);
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
        cursor += sizeof(Int64);
        return;
    case TiDB::CodecFlagDecimal:
        SkipDecimal(cursor, raw_value);
        return;
    case TiDB::CodecFlagJson:
        JsonBinary::SkipJson(cursor, raw_value);
        return;
    case TiDB::CodecFlagVectorFloat32:
        SkipVectorFloat32(cursor, raw_value);
        return;
    default:
        throw Exception("Unknown Type:" + std::to_string(raw_value[cursor - 1]), ErrorCodes::LOGICAL_ERROR);
    }
}

void EncodeFloat64(Float64 num, WriteBuffer & ss)
{
    auto u = enforce_cast<UInt64>(num);
    if (u & SIGN_MASK)
        u = ~u;
    else
        u |= SIGN_MASK;
    return EncodeUInt<UInt64>(u, ss);
}

void EncodeBytes(const String & ori_str, WriteBuffer & ss)
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
        ss.write(static_cast<char>(ENC_MARKER - static_cast<UInt8>(pad)));
        index += ENC_GROUP_SIZE;
    }
}

void EncodeCompactBytes(const String & str, WriteBuffer & ss)
{
    TiKV::writeVarInt(static_cast<Int64>(str.size()), ss);
    ss.write(str.c_str(), str.size());
}

void EncodeJSON(const String & str, WriteBuffer & ss)
{
    // TiFlash store the JSON binary as string, so just return the string
    ss.write(str.c_str(), str.size());
}

void EncodeVarUInt(UInt64 num, WriteBuffer & ss)
{
    TiKV::writeVarUInt(num, ss);
}

void EncodeVarInt(Int64 num, WriteBuffer & ss)
{
    TiKV::writeVarInt(num, ss);
}

inline void writeWord(String & buf, Int32 word, int size)
{
    switch (size)
    {
    case 1:
        buf.push_back(static_cast<char>(word));
        break;
    case 2:
        buf.push_back(static_cast<char>(word >> 8));
        buf.push_back(static_cast<char>(word));
        break;
    case 3:
        buf.push_back(static_cast<char>(word >> 16));
        buf.push_back(static_cast<char>(word >> 8));
        buf.push_back(static_cast<char>(word));
        break;
    case 4:
        buf.push_back(static_cast<char>(word >> 24));
        buf.push_back(static_cast<char>(word >> 16));
        buf.push_back(static_cast<char>(word >> 8));
        buf.push_back(static_cast<char>(word));
        break;
    }
}

template <typename T>
void EncodeDecimalImpl(const T & dec, PrecType prec, ScaleType frac, WriteBuffer & ss)
{
    static_assert(IsDecimal<T>);

    // Scale must (if not, then we have bugs) be the same as TiDB expected, but precision will be
    // trimmed to as minimal as possible by TiFlash decimal implementation. TiDB doesn't allow
    // decimal with precision less than scale, therefore in theory we should align value's precision
    // according to data type. But TiDB somehow happens to allow precision not equal to data type,
    // of which we take advantage to make such a handy fix.
    if (prec < frac)
    {
        prec = frac;
    }
    constexpr Int32 decimal_mod = powers10[digitsPerWord];
    ss.write(static_cast<UInt8>(prec));
    ss.write(static_cast<UInt8>(frac));

    int digits_int = prec - frac;
    int words_int = digits_int / digitsPerWord;
    int leading_digits = digits_int - words_int * digitsPerWord;
    int words_frac = frac / digitsPerWord;
    int trailing_digits = frac - words_frac * digitsPerWord;
    int words = getWords(prec, frac);

    Int256 value = dec.value;
    Int32 mask = 0;
    if (value < 0)
    {
        value = -value;
        mask = -1;
    }

    std::vector<Int32> v;

    for (int i = 0; i < words; i++)
    {
        if (i == 0 && trailing_digits > 0)
        {
            v.push_back(static_cast<Int32>(value % powers10[trailing_digits]));
            value /= powers10[trailing_digits];
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
        if (i == 0 && leading_digits > 0)
        {
            int size = dig2Bytes[leading_digits];
            writeWord(buf, v[i], size);
        }
        else if (i + 1 == words && trailing_digits > 0)
        {
            int size = dig2Bytes[trailing_digits];
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

void EncodeDecimalForRow(const Field & field, WriteBuffer & ss, const ColumnInfo & column_info)
{
    if (field.getType() == Field::Types::Decimal32)
    {
        auto decimal_field = field.get<DecimalField<Decimal32>>();
        return EncodeDecimalImpl(decimal_field.getValue(), column_info.flen, column_info.decimal, ss);
    }
    else if (field.getType() == Field::Types::Decimal64)
    {
        auto decimal_field = field.get<DecimalField<Decimal64>>();
        return EncodeDecimalImpl(decimal_field.getValue(), column_info.flen, column_info.decimal, ss);
    }
    else if (field.getType() == Field::Types::Decimal128)
    {
        auto decimal_field = field.get<DecimalField<Decimal128>>();
        return EncodeDecimalImpl(decimal_field.getValue(), column_info.flen, column_info.decimal, ss);
    }
    else if (field.getType() == Field::Types::Decimal256)
    {
        const auto & decimal_field = field.get<DecimalField<Decimal256>>();
        return EncodeDecimalImpl(decimal_field.getValue(), column_info.flen, column_info.decimal, ss);
    }
    else
    {
        throw Exception("Not a decimal when decoding decimal", ErrorCodes::LOGICAL_ERROR);
    }
}

void EncodeDecimal(const Field & field, WriteBuffer & ss)
{
    if (field.getType() == Field::Types::Decimal32)
    {
        auto decimal_field = field.get<DecimalField<Decimal32>>();
        return EncodeDecimalImpl(decimal_field.getValue(), decimal_field.getPrec(), decimal_field.getScale(), ss);
    }
    else if (field.getType() == Field::Types::Decimal64)
    {
        auto decimal_field = field.get<DecimalField<Decimal64>>();
        return EncodeDecimalImpl(decimal_field.getValue(), decimal_field.getPrec(), decimal_field.getScale(), ss);
    }
    else if (field.getType() == Field::Types::Decimal128)
    {
        auto decimal_field = field.get<DecimalField<Decimal128>>();
        return EncodeDecimalImpl(decimal_field.getValue(), decimal_field.getPrec(), decimal_field.getScale(), ss);
    }
    else if (field.getType() == Field::Types::Decimal256)
    {
        const auto & decimal_field = field.get<DecimalField<Decimal256>>();
        return EncodeDecimalImpl(decimal_field.getValue(), decimal_field.getPrec(), decimal_field.getScale(), ss);
    }
    else
    {
        throw Exception("Not a decimal when decoding decimal", ErrorCodes::LOGICAL_ERROR);
    }
}

void EncodeDatumForRow(const Field & field, TiDB::CodecFlag flag, WriteBuffer & ss, const ColumnInfo & column_info)
{
    if (flag == TiDB::CodecFlagDecimal && !field.isNull())
    {
        EncodeUInt(static_cast<UInt8>(flag), ss);
        return EncodeDecimalForRow(field, ss, column_info);
    }
    return EncodeDatum(field, flag, ss);
}

void EncodeDatum(const Field & field, TiDB::CodecFlag flag, WriteBuffer & ss)
{
    if (field.isNull())
        flag = TiDB::CodecFlagNil;
    EncodeUInt(static_cast<UInt8>(flag), ss);
    switch (flag)
    {
    case TiDB::CodecFlagDecimal:
        return EncodeDecimal(field, ss);
    case TiDB::CodecFlagCompactBytes:
        return EncodeCompactBytes(field.safeGet<String>(), ss);
    case TiDB::CodecFlagFloat:
        return EncodeFloat64(field.safeGet<Float64>(), ss);
    case TiDB::CodecFlagUInt:
        return EncodeUInt<UInt64>(field.safeGet<UInt64>(), ss);
    case TiDB::CodecFlagInt:
        return EncodeInt64(field.safeGet<Int64>(), ss);
    case TiDB::CodecFlagVarInt:
        return EncodeVarInt(field.safeGet<Int64>(), ss);
    case TiDB::CodecFlagVarUInt:
        return EncodeVarUInt(field.safeGet<UInt64>(), ss);
    case TiDB::CodecFlagDuration:
        return EncodeInt64(field.safeGet<Int64>(), ss);
    case TiDB::CodecFlagJson:
        return EncodeJSON(field.safeGet<String>(), ss);
    case TiDB::CodecFlagVectorFloat32:
        return EncodeVectorFloat32(field.safeGet<Array>(), ss);
    case TiDB::CodecFlagNil:
        return;
    default:
        throw Exception("Not implemented codec flag: " + std::to_string(flag), ErrorCodes::LOGICAL_ERROR);
    }
}

template void EncodeDecimalImpl<Decimal32>(const Decimal32 &, PrecType, ScaleType, WriteBuffer & ss);
template void EncodeDecimalImpl<Decimal64>(const Decimal64 &, PrecType, ScaleType, WriteBuffer & ss);
template void EncodeDecimalImpl<Decimal128>(const Decimal128 &, PrecType, ScaleType, WriteBuffer & ss);
template void EncodeDecimalImpl<Decimal256>(const Decimal256 &, PrecType, ScaleType, WriteBuffer & ss);

} // namespace DB

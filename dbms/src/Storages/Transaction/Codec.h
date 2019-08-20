#pragma once

#include <Common/Decimal.h>
#include <Core/Field.h>
#include <IO/Endian.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

static const size_t ENC_GROUP_SIZE = 8;
static const UInt8 ENC_MARKER = static_cast<UInt8>(0xff);
static const char ENC_ASC_PADDING[ENC_GROUP_SIZE] = {0};

template <typename T>
inline T DecodeNumber(size_t & cursor, const String & raw_value)
{
    T res = readBigEndian<T>(&raw_value[cursor]);
    cursor += sizeof(T);
    return res;
}

Float64 DecodeFloat64(size_t & cursor, const String & raw_value);

String DecodeBytes(size_t & cursor, const String & raw_value);

String DecodeCompactBytes(size_t & cursor, const String & raw_value);

Int64 DecodeVarInt(size_t & cursor, const String & raw_value);

UInt64 DecodeVarUInt(size_t & cursor, const String & raw_value);

Field DecodeDecimal(size_t & cursor, const String & raw_value);

Field DecodeDatum(size_t & cursor, const String & raw_value);

template <typename T>
inline void EncodeNumber(T u, std::stringstream & ss)
{
    u = toBigEndian(u);
    ss.write(reinterpret_cast<const char *>(&u), sizeof(u));
}

void EncodeFloat64(Float64 num, std::stringstream & ss);

void EncodeBytes(const String & ori_str, std::stringstream & ss);

void EncodeCompactBytes(const String & str, std::stringstream & ss);

void EncodeVarInt(Int64 num, std::stringstream & ss);

void EncodeVarUInt(UInt64 num, std::stringstream & ss);

template <typename T>
void EncodeDecimal(const T & dec, PrecType prec, ScaleType frac, std::stringstream & ss);

void EncodeDatum(const Field & field, TiDB::CodecFlag flag, std::stringstream & ss);

} // namespace DB

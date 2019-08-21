#pragma once

#include <Common/Decimal.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <IO/Endian.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

static const size_t ENC_GROUP_SIZE = 8;
static const UInt8 ENC_MARKER = static_cast<UInt8>(0xff);
static const char ENC_ASC_PADDING[ENC_GROUP_SIZE] = {0};

static const UInt64 SIGN_MASK = UInt64(1) << 63;

template <typename T>
inline std::enable_if_t<std::is_unsigned_v<T>, T> DecodeUInt(size_t & cursor, const String & raw_value)
{
    T res = readBigEndian<T>(&raw_value[cursor]);
    cursor += sizeof(T);
    return res;
}

inline Int64 DecodeInt64(size_t & cursor, const String & raw_value)
{
    return static_cast<Int64>(DecodeUInt<UInt64>(cursor, raw_value) ^ SIGN_MASK);
}

Float64 DecodeFloat64(size_t & cursor, const String & raw_value);

String DecodeBytes(size_t & cursor, const String & raw_value);

String DecodeCompactBytes(size_t & cursor, const String & raw_value);

Int64 DecodeVarInt(size_t & cursor, const String & raw_value);

UInt64 DecodeVarUInt(size_t & cursor, const String & raw_value);

Decimal DecodeDecimal(size_t & cursor, const String & raw_value);

Field DecodeDatum(size_t & cursor, const String & raw_value);

template <typename T>
inline std::enable_if_t<std::is_unsigned_v<T>, void> EncodeUInt(T u, std::stringstream & ss)
{
    u = toBigEndian(u);
    ss.write(reinterpret_cast<const char *>(&u), sizeof(u));
}

inline void EncodeInt64(Int64 i, std::stringstream & ss) { EncodeUInt<UInt64>(static_cast<UInt64>(i) ^ SIGN_MASK, ss); }

void EncodeFloat64(Float64 num, std::stringstream & ss);

void EncodeBytes(const String & ori_str, std::stringstream & ss);

void EncodeCompactBytes(const String & str, std::stringstream & ss);

void EncodeVarInt(Int64 num, std::stringstream & ss);

void EncodeVarUInt(UInt64 num, std::stringstream & ss);

void EncodeDecimal(const Decimal & dec, std::stringstream & ss);

void EncodeDatum(const Field & field, TiDB::CodecFlag flag, std::stringstream & ss, const DataTypePtr & ch_type = nullptr);

} // namespace DB

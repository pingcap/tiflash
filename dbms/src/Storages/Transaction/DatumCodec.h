#pragma once

#include <Common/Decimal.h>
#include <Core/Field.h>
#include <IO/Endian.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>

/// Functions in this file are used for individual datum codec, i.e. UInt/Int64, Float64, String/Bytes, Decimal, Enum, Set, etc.
/// The internal representation of a datum in TiFlash is Field.
/// Encoded/decoded datum must be from/to DatumBumpy/Flat classes to do flatten/unflatten.
/// These functions are used across the storage (row codec in RowCodec.h/cpp) and compute (coprocessor codec under Coprocessor directory) layers.
/// But be noted that each layer could have their own datum codec implementations other than this file for endianness or specific data types - TiDB does so, thus so does TiFlash.
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

UInt64 DecodeVarUInt(size_t & cursor, const String & raw_value);

Int64 DecodeVarInt(size_t & cursor, const String & raw_value);

Field DecodeDecimal(size_t & cursor, const String & raw_value);

Field DecodeDecimalForCHRow(size_t & cursor, const String & raw_value, const TiDB::ColumnInfo & column_info);

Field DecodeDatum(size_t & cursor, const String & raw_value);

Field DecodeDatumForCHRow(size_t & cursor, const String & raw_value, const TiDB::ColumnInfo & column_info);

void SkipDatum(size_t & cursor, const String & raw_value);

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

void EncodeJSON(const String & str, std::stringstream & ss);

void EncodeVarUInt(UInt64 num, std::stringstream & ss);

void EncodeVarInt(Int64 num, std::stringstream & ss);

void EncodeDecimal(const Field & field, std::stringstream & ss);

void EncodeDecimalForRow(const Field & field, std::stringstream & ss, const ColumnInfo & column_info);

void EncodeDatum(const Field & field, TiDB::CodecFlag flag, std::stringstream & ss);

void EncodeDatumForRow(const Field & field, TiDB::CodecFlag flag, std::stringstream & ss, const ColumnInfo & column_info);

} // namespace DB

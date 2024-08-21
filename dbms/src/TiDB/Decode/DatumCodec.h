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

#pragma once

#include <Common/Decimal.h>
#include <Core/Field.h>
#include <IO/Buffer/WriteBuffer.h>
#include <IO/Endian.h>
#include <TiDB/Schema/TiDBTypes.h>
#include <TiDB/Schema/TiDB_fwd.h>

/// Functions in this file are used for individual datum codec, i.e. UInt/Int64, Float64, String/Bytes, Decimal, Enum, Set, etc.
/// The internal representation of a datum in TiFlash is Field.
/// Encoded/decoded datum must be from/to DatumBumpy/Flat classes to do flatten/unflatten.
/// These functions are used across the storage (row codec in RowCodec.h/cpp) and compute (coprocessor codec under Coprocessor directory) layers.
/// But be noted that each layer could have their own datum codec implementations other than this file for endianness or specific data types - TiDB does so, thus so does TiFlash.
namespace DB
{
static constexpr size_t ENC_GROUP_SIZE = 8;
static constexpr UInt8 ENC_MARKER = static_cast<UInt8>(0xff);
static constexpr char ENC_ASC_PADDING[ENC_GROUP_SIZE] = {0};

static constexpr UInt64 SIGN_MASK = static_cast<UInt64>(1) << 63;

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

UInt64 DecodeVarUInt(size_t & cursor, const StringRef & raw_value);

Int64 DecodeVarInt(size_t & cursor, const String & raw_value);

Field DecodeVectorFloat32(size_t & cursor, const String & raw_value);

Field DecodeDecimal(size_t & cursor, const String & raw_value);

Field DecodeDecimalForCHRow(size_t & cursor, const String & raw_value, const TiDB::ColumnInfo & column_info);

Field DecodeDatum(size_t & cursor, const String & raw_value);

Field DecodeDatumForCHRow(size_t & cursor, const String & raw_value, const TiDB::ColumnInfo & column_info);

void SkipDatum(size_t & cursor, const String & raw_value);

template <typename T>
inline std::enable_if_t<std::is_unsigned_v<T>, void> EncodeUInt(T u, WriteBuffer & ss)
{
    u = toBigEndian(u);
    ss.write(reinterpret_cast<const char *>(&u), sizeof(u));
}

inline void EncodeInt64(Int64 i, WriteBuffer & ss)
{
    EncodeUInt<UInt64>(static_cast<UInt64>(i) ^ SIGN_MASK, ss);
}

void EncodeFloat64(Float64 num, WriteBuffer & ss);

void EncodeBytes(const String & ori_str, WriteBuffer & ss);

void EncodeCompactBytes(const String & str, WriteBuffer & ss);

void EncodeJSON(const String & str, WriteBuffer & ss);

void EncodeVectorFloat32(const Array & val, WriteBuffer & ss);

void EncodeVarUInt(UInt64 num, WriteBuffer & ss);

void EncodeVarInt(Int64 num, WriteBuffer & ss);

void EncodeDecimal(const Field & field, WriteBuffer & ss);

void EncodeDecimalForRow(const Field & field, WriteBuffer & ss, const TiDB::ColumnInfo & column_info);

void EncodeDatum(const Field & field, TiDB::CodecFlag flag, WriteBuffer & ss);

void EncodeDatumForRow(
    const Field & field,
    TiDB::CodecFlag flag,
    WriteBuffer & ss,
    const TiDB::ColumnInfo & column_info);

} // namespace DB

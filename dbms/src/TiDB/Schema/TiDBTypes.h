// Copyright 2024 PingCAP, Inc.
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

namespace TiDB
{

// Column types.
// In format:
// TiDB type, int value, codec flag, CH type.
#ifdef M
#error "Please undefine macro M first."
#endif
#define COLUMN_TYPES(M)                       \
    M(Decimal, 0, Decimal, Decimal32)         \
    M(Tiny, 1, VarInt, Int8)                  \
    M(Short, 2, VarInt, Int16)                \
    M(Long, 3, VarInt, Int32)                 \
    M(Float, 4, Float, Float32)               \
    M(Double, 5, Float, Float64)              \
    M(Null, 6, Nil, Nothing)                  \
    M(Timestamp, 7, UInt, MyDateTime)         \
    M(LongLong, 8, Int, Int64)                \
    M(Int24, 9, VarInt, Int32)                \
    M(Date, 10, UInt, MyDate)                 \
    M(Time, 11, Duration, Int64)              \
    M(Datetime, 12, UInt, MyDateTime)         \
    M(Year, 13, Int, Int16)                   \
    M(NewDate, 14, Int, MyDate)               \
    M(Varchar, 15, CompactBytes, String)      \
    M(Bit, 16, VarInt, UInt64)                \
    M(JSON, 0xf5, Json, String)               \
    M(NewDecimal, 0xf6, Decimal, Decimal32)   \
    M(Enum, 0xf7, VarUInt, Enum16)            \
    M(Set, 0xf8, VarUInt, UInt64)             \
    M(TinyBlob, 0xf9, CompactBytes, String)   \
    M(MediumBlob, 0xfa, CompactBytes, String) \
    M(LongBlob, 0xfb, CompactBytes, String)   \
    M(Blob, 0xfc, CompactBytes, String)       \
    M(VarString, 0xfd, CompactBytes, String)  \
    M(String, 0xfe, CompactBytes, String)     \
    M(Geometry, 0xff, CompactBytes, String)

enum TP
{
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct) Type##tt = (v),
    COLUMN_TYPES(M)
#undef M
};


// Codec flags.
// In format: TiDB codec flag, int value.
#ifdef M
#error "Please undefine macro M first."
#endif
#define CODEC_FLAGS(M) \
    M(Nil, 0)          \
    M(Bytes, 1)        \
    M(CompactBytes, 2) \
    M(Int, 3)          \
    M(UInt, 4)         \
    M(Float, 5)        \
    M(Decimal, 6)      \
    M(Duration, 7)     \
    M(VarInt, 8)       \
    M(VarUInt, 9)      \
    M(Json, 10)        \
    M(Max, 250)

enum CodecFlag
{
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(cf, v) CodecFlag##cf = (v),
    CODEC_FLAGS(M)
#undef M
};

} // namespace TiDB

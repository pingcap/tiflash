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

#include <Poco/Types.h>
#include <common/types.h>

#include <limits>
#include <string>
#include <vector>

/// Remove the population of thread_local from Poco
#ifdef thread_local
#undef thread_local
#endif

namespace DB
{
/// Data types for representing elementary values from a database in RAM.

struct Null
{
};

template <typename T>
struct TypeName;

template <>
struct TypeName<UInt8>
{
    static const char * get() { return "UInt8"; }
};
template <>
struct TypeName<UInt16>
{
    static const char * get() { return "UInt16"; }
};
template <>
struct TypeName<UInt32>
{
    static const char * get() { return "UInt32"; }
};
template <>
struct TypeName<UInt64>
{
    static const char * get() { return "UInt64"; }
};
template <>
struct TypeName<UInt128>
{
    static const char * get() { return "UInt128"; }
};
template <>
struct TypeName<Int8>
{
    static const char * get() { return "Int8"; }
};
template <>
struct TypeName<Int16>
{
    static const char * get() { return "Int16"; }
};
template <>
struct TypeName<Int32>
{
    static const char * get() { return "Int32"; }
};
template <>
struct TypeName<Int64>
{
    static const char * get() { return "Int64"; }
};
template <>
struct TypeName<Int128>
{
    static const char * get() { return "Int128"; }
};
template <>
struct TypeName<Int256>
{
    static const char * get() { return "Int256"; }
};
template <>
struct TypeName<Int512>
{
    static const char * get() { return "Int512"; }
};
template <>
struct TypeName<Float32>
{
    static const char * get() { return "Float32"; }
};
template <>
struct TypeName<Float64>
{
    static const char * get() { return "Float64"; }
};
template <>
struct TypeName<String>
{
    static const char * get() { return "String"; }
};

/// Not a data type in database, defined just for convenience.
using Strings = std::vector<String>;

enum class TypeIndex
{
    Nothing = 0,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    Float32,
    Float64,
    Date,
    DateTime,
    String,
    FixedString,
    Enum8,
    Enum16,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    UUID,
    Array,
    Tuple,
    Set,
    Interval,
    Nullable,
    Function,
    AggregateFunction,
    LowCardinality,
    MyDate,
    MyDateTime,
    MyTimeStamp,
    MyTime
};

template <typename T>
struct TypeId;
template <>
struct TypeId<UInt8>
{
    static constexpr const TypeIndex value = TypeIndex::UInt8;
};
template <>
struct TypeId<UInt16>
{
    static constexpr const TypeIndex value = TypeIndex::UInt16;
};
template <>
struct TypeId<UInt32>
{
    static constexpr const TypeIndex value = TypeIndex::UInt32;
};
template <>
struct TypeId<UInt64>
{
    static constexpr const TypeIndex value = TypeIndex::UInt64;
};
template <>
struct TypeId<UInt128>
{
    static constexpr const TypeIndex value = TypeIndex::UInt128;
};
template <>
struct TypeId<Int8>
{
    static constexpr const TypeIndex value = TypeIndex::Int8;
};
template <>
struct TypeId<Int16>
{
    static constexpr const TypeIndex value = TypeIndex::Int16;
};
template <>
struct TypeId<Int32>
{
    static constexpr const TypeIndex value = TypeIndex::Int32;
};
template <>
struct TypeId<Int64>
{
    static constexpr const TypeIndex value = TypeIndex::Int64;
};
template <>
struct TypeId<Int128>
{
    static constexpr const TypeIndex value = TypeIndex::Int128;
};
template <>
struct TypeId<Int256>
{
    static constexpr const TypeIndex value = TypeIndex::Int256;
};
template <>
struct TypeId<Float32>
{
    static constexpr const TypeIndex value = TypeIndex::Float32;
};
template <>
struct TypeId<Float64>
{
    static constexpr const TypeIndex value = TypeIndex::Float64;
};

/// Avoid to use `std::vector<bool`
using BoolVec = std::vector<UInt8>;

} // namespace DB

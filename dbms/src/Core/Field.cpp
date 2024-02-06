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

#include <Common/FieldVisitors.h>
#include <Core/DecimalComparison.h>
#include <Core/Field.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <fmt/core.h>


namespace DB
{
void readBinary(Array & x, ReadBuffer & buf)
{
    size_t size;
    UInt8 type;
    DB::readBinary(type, buf);
    DB::readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        switch (type)
        {
        case Field::Types::Null:
        {
            x.push_back(DB::Field());
            break;
        }
        case Field::Types::UInt64:
        {
            UInt64 value;
            DB::readVarUInt(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::UInt128:
        {
            UInt128 value{};
            DB::readBinary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Int64:
        {
            Int64 value;
            DB::readVarInt(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Float64:
        {
            Float64 value;
            DB::readFloatBinary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::String:
        {
            std::string value;
            DB::readStringBinary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Array:
        {
            Array value;
            DB::readBinary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Tuple:
        {
            Tuple value;
            DB::readBinary(value, buf);
            x.push_back(value);
            break;
        }
        };
    }
}

void writeBinary(const Array & x, WriteBuffer & buf)
{
    UInt8 type = Field::Types::Null;
    size_t size = x.size();
    if (size)
        type = x.front().getType();
    DB::writeBinary(type, buf);
    DB::writeBinary(size, buf);

    for (const auto & i : x)
    {
        switch (type)
        {
        case Field::Types::Null:
            break;
        case Field::Types::UInt64:
        {
            DB::writeVarUInt(get<UInt64>(i), buf);
            break;
        }
        case Field::Types::UInt128:
        {
            DB::writeBinary(get<UInt128>(i), buf);
            break;
        }
        case Field::Types::Int64:
        {
            DB::writeVarInt(get<Int64>(i), buf);
            break;
        }
        case Field::Types::Float64:
        {
            DB::writeFloatBinary(get<Float64>(i), buf);
            break;
        }
        case Field::Types::String:
        {
            DB::writeStringBinary(get<std::string>(i), buf);
            break;
        }
        case Field::Types::Array:
        {
            DB::writeBinary(get<Array>(i), buf);
            break;
        }
        case Field::Types::Tuple:
        {
            DB::writeBinary(get<Tuple>(i), buf);
            break;
        }
        };
    }
}

void writeText(const Array & x, WriteBuffer & buf)
{
    DB::String res = applyVisitor(DB::FieldVisitorToString(), DB::Field(x));
    buf.write(res.data(), res.size());
}

void readBinary(Tuple & x_def, ReadBuffer & buf)
{
    auto & x = x_def.toUnderType();
    size_t size;
    DB::readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        DB::readBinary(type, buf);

        switch (type)
        {
        case Field::Types::Null:
        {
            x.push_back(DB::Field());
            break;
        }
        case Field::Types::UInt64:
        {
            UInt64 value;
            DB::readVarUInt(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::UInt128:
        {
            UInt128 value{};
            DB::readBinary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Int64:
        {
            Int64 value;
            DB::readVarInt(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Float64:
        {
            Float64 value;
            DB::readFloatBinary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::String:
        {
            std::string value;
            DB::readStringBinary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Array:
        {
            Array value;
            DB::readBinary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Tuple:
        {
            Tuple value;
            DB::readBinary(value, buf);
            x.push_back(value);
            break;
        }
        };
    }
}

void writeBinary(const Tuple & x_def, WriteBuffer & buf)
{
    const auto & x = x_def.toUnderType();
    const size_t size = x.size();
    DB::writeBinary(size, buf);

    for (const auto & i : x)
    {
        const UInt8 type = i.getType();
        DB::writeBinary(type, buf);

        switch (type)
        {
        case Field::Types::Null:
            break;
        case Field::Types::UInt64:
        {
            DB::writeVarUInt(get<UInt64>(i), buf);
            break;
        }
        case Field::Types::UInt128:
        {
            DB::writeBinary(get<UInt128>(i), buf);
            break;
        }
        case Field::Types::Int64:
        {
            DB::writeVarInt(get<Int64>(i), buf);
            break;
        }
        case Field::Types::Float64:
        {
            DB::writeFloatBinary(get<Float64>(i), buf);
            break;
        }
        case Field::Types::String:
        {
            DB::writeStringBinary(get<std::string>(i), buf);
            break;
        }
        case Field::Types::Array:
        {
            DB::writeBinary(get<Array>(i), buf);
            break;
        }
        case Field::Types::Tuple:
        {
            DB::writeBinary(get<Tuple>(i), buf);
            break;
        }
        };
    }
}

void writeText(const Tuple & x, WriteBuffer & buf)
{
    DB::String res = applyVisitor(DB::FieldVisitorToString(), DB::Field(x));
    buf.write(res.data(), res.size());
}

template <typename T>
static bool decEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
static bool decLess(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
static bool decLessOrEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, LessOrEqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <>
bool decimalEqual(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale)
{
    return decEqual(x, y, x_scale, y_scale);
}
template <>
bool decimalLess(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale)
{
    return decLess(x, y, x_scale, y_scale);
}
template <>
bool decimalLessOrEqual(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale)
{
    return decLessOrEqual(x, y, x_scale, y_scale);
}

template <>
bool decimalEqual(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale)
{
    return decEqual(x, y, x_scale, y_scale);
}
template <>
bool decimalLess(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale)
{
    return decLess(x, y, x_scale, y_scale);
}
template <>
bool decimalLessOrEqual(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale)
{
    return decLessOrEqual(x, y, x_scale, y_scale);
}

template <>
bool decimalEqual(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale)
{
    return decEqual(x, y, x_scale, y_scale);
}
template <>
bool decimalLess(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale)
{
    return decLess(x, y, x_scale, y_scale);
}
template <>
bool decimalLessOrEqual(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale)
{
    return decLessOrEqual(x, y, x_scale, y_scale);
}

template <>
bool decimalEqual(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale)
{
    return decEqual(x, y, x_scale, y_scale);
}
template <>
bool decimalLess(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale)
{
    return decLess(x, y, x_scale, y_scale);
}
template <>
bool decimalLessOrEqual(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale)
{
    return decLessOrEqual(x, y, x_scale, y_scale);
}

String Field::toString() const
{
    return applyVisitor(DB::FieldVisitorDump(), *this);
}

} // namespace DB

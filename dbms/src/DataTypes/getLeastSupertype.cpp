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

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionHelpers.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NO_COMMON_TYPE;
} // namespace ErrorCodes

namespace
{
String getExceptionMessagePrefix(const DataTypes & types)
{
    WriteBufferFromOwnString res;
    res << "There is no supertype for types ";

    bool first = true;
    for (const auto & type : types)
    {
        if (!first)
            res << ", ";
        first = false;

        res << type->getName();
    }

    return res.str();
}
} // namespace


DataTypePtr getLeastSupertype(const DataTypes & types)
{
    /// Trivial cases

    if (types.empty())
        return std::make_shared<DataTypeNothing>();

    if (types.size() == 1)
        return types[0];

    /// All types are equal
    {
        bool all_equal = true;
        for (size_t i = 1, size = types.size(); i < size; ++i)
        {
            if (!types[i]->equals(*types[0]))
            {
                all_equal = false;
                break;
            }
        }

        if (all_equal)
            return types[0];
    }

    /// Recursive rules

    /// If there are Nothing types, skip them
    {
        DataTypes non_nothing_types;
        non_nothing_types.reserve(types.size());

        for (const auto & type : types)
            if (!typeid_cast<const DataTypeNothing *>(type.get()))
                non_nothing_types.emplace_back(type);

        if (non_nothing_types.size() < types.size())
            return getLeastSupertype(non_nothing_types);
    }

    /// For tuples
    {
        bool have_tuple = false;
        bool all_tuples = true;
        size_t tuple_size = 0;

        std::vector<DataTypes> nested_types;

        for (const auto & type : types)
        {
            if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
            {
                if (!have_tuple)
                {
                    tuple_size = type_tuple->getElements().size();
                    nested_types.resize(tuple_size);
                    for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                        nested_types[elem_idx].reserve(types.size());
                }
                else if (tuple_size != type_tuple->getElements().size())
                    throw Exception(
                        getExceptionMessagePrefix(types) + " because Tuples have different sizes",
                        ErrorCodes::NO_COMMON_TYPE);

                have_tuple = true;

                for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                    nested_types[elem_idx].emplace_back(type_tuple->getElements()[elem_idx]);
            }
            else
                all_tuples = false;
        }

        if (have_tuple)
        {
            if (!all_tuples)
                throw Exception(
                    getExceptionMessagePrefix(types) + " because some of them are Tuple and some of them are not",
                    ErrorCodes::NO_COMMON_TYPE);

            DataTypes common_tuple_types(tuple_size);
            for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                common_tuple_types[elem_idx] = getLeastSupertype(nested_types[elem_idx]);

            return std::make_shared<DataTypeTuple>(common_tuple_types);
        }
    }

    /// For Nullable
    {
        bool have_nullable = false;

        DataTypes nested_types;
        nested_types.reserve(types.size());

        for (const auto & type : types)
        {
            if (const auto * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
            {
                have_nullable = true;

                if (!type_nullable->onlyNull())
                    nested_types.emplace_back(type_nullable->getNestedType());
            }
            else
                nested_types.emplace_back(type);
        }

        if (have_nullable)
        {
            return std::make_shared<DataTypeNullable>(getLeastSupertype(nested_types));
        }
    }

    /// For Arrays, canBeInsideNullable = true, should check it after handling Nullable
    {
        bool have_array = false;
        bool all_arrays = true;

        DataTypes nested_types;
        nested_types.reserve(types.size());

        for (const auto & type : types)
        {
            if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
            {
                have_array = true;
                nested_types.emplace_back(type_array->getNestedType());
            }
            else
                all_arrays = false;
        }

        if (have_array)
        {
            if (!all_arrays)
                throw Exception(
                    getExceptionMessagePrefix(types) + " because some of them are Array and some of them are not",
                    ErrorCodes::NO_COMMON_TYPE);

            return std::make_shared<DataTypeArray>(getLeastSupertype(nested_types));
        }
    }

    /// Non-recursive rules

    std::unordered_set<TypeIndex> type_ids;
    for (const auto & type : types)
        type_ids.insert(type->getTypeId());

    /// For String and FixedString, or for different FixedStrings, the common type is String.
    /// No other types are compatible with Strings. TODO Enums?
    {
        UInt32 have_string = type_ids.count(TypeIndex::String);
        UInt32 have_fixed_string = type_ids.count(TypeIndex::FixedString);

        if (have_string || have_fixed_string)
        {
            bool all_strings = type_ids.size() == (have_string + have_fixed_string);
            if (!all_strings)
                throw Exception(
                    getExceptionMessagePrefix(types)
                        + " because some of them are String/FixedString and some of them are not",
                    ErrorCodes::NO_COMMON_TYPE);

            return std::make_shared<DataTypeString>();
        }
    }

    /// For Date and DateTime, the common type is DateTime. No other types are compatible.
    {
        UInt32 have_date = type_ids.count(TypeIndex::Date);
        UInt32 have_datetime = type_ids.count(TypeIndex::DateTime);
        UInt32 have_my_date = type_ids.count(TypeIndex::MyDate);
        UInt32 have_my_datetime = type_ids.count(TypeIndex::MyDateTime);

        if (have_date || have_datetime || have_my_date || have_my_datetime)
        {
            if (have_date + have_datetime && have_my_date + have_my_datetime)
                throw Exception(
                    getExceptionMessagePrefix(types)
                        + " because Date/Datetime and MyDate/MyDatetime are not compatible",
                    ErrorCodes::NO_COMMON_TYPE);
            if (have_date + have_datetime == type_ids.size())
                return std::make_shared<DataTypeDateTime>();
            if (have_my_date + have_my_datetime == type_ids.size())
            {
                int fsp = 0;
                for (const auto & type : types)
                {
                    const auto * datetime_type = checkAndGetDataType<DataTypeMyDateTime>(type.get());
                    if (datetime_type)
                        fsp = std::max(fsp, datetime_type->getFraction());
                }
                return std::make_shared<DataTypeMyDateTime>(fsp);
            }

            throw Exception(
                getExceptionMessagePrefix(types) + " because some of them are Date/DateTime and some of them are not",
                ErrorCodes::NO_COMMON_TYPE);
        }
    }

    /// For Duration, the common type is Duration with bigger fsp. No other types are compatible.
    {
        UInt32 have_duration = type_ids.count(TypeIndex::MyTime);

        if (have_duration)
        {
            if (have_duration == type_ids.size())
            {
                int fsp = 0;
                for (const auto & type : types)
                {
                    fsp = std::max(fsp, checkAndGetDataType<DataTypeMyDuration>(type.get())->getFsp());
                }
                return std::make_shared<DataTypeMyDuration>(fsp);
            }

            throw Exception(
                getExceptionMessagePrefix(types) + " because some of them are MyDuration and some of them are not",
                ErrorCodes::NO_COMMON_TYPE);
        }
    }

    /// Decimals
    {
        UInt32 have_decimal32 = type_ids.count(TypeIndex::Decimal32);
        UInt32 have_decimal64 = type_ids.count(TypeIndex::Decimal64);
        UInt32 have_decimal128 = type_ids.count(TypeIndex::Decimal128);
        UInt32 have_decimal256 = type_ids.count(TypeIndex::Decimal256);

        if (have_decimal32 || have_decimal64 || have_decimal128 || have_decimal256)
        {
            UInt32 num_supported = have_decimal32 + have_decimal64 + have_decimal128 + have_decimal256;

            std::vector<TypeIndex> int_ids
                = {TypeIndex::Int8,
                   TypeIndex::UInt8,
                   TypeIndex::Int16,
                   TypeIndex::UInt16,
                   TypeIndex::Int32,
                   TypeIndex::UInt32,
                   TypeIndex::Int64,
                   TypeIndex::UInt64};
            std::vector<UInt32> num_ints(int_ids.size(), 0);

            TypeIndex max_int = TypeIndex::Nothing;
            for (size_t i = 0; i < int_ids.size(); ++i)
            {
                UInt32 num = type_ids.count(int_ids[i]);
                num_ints[i] = num;
                num_supported += num;
                if (num)
                    max_int = int_ids[i];
            }

            /// decimal and float is not compatible
            if (num_supported != type_ids.size())
                throw Exception(
                    getExceptionMessagePrefix(types) + " because some of them have no lossless convertion to Decimal",
                    ErrorCodes::NO_COMMON_TYPE);

            UInt32 max_scale = 0;
            UInt32 max_int_part = 0;
            for (const auto & type : types)
            {
                if (IsDecimalDataType(type))
                {
                    UInt32 scale = getDecimalScale(*type, 0);
                    UInt32 prec = getDecimalPrecision(*type, 0);
                    if (scale > max_scale)
                        max_scale = scale;
                    if (prec - scale > max_int_part)
                        max_int_part = prec - scale;
                }
            }
            max_int_part = std::max(max_int_part, leastDecimalPrecisionFor(max_int));

            UInt32 min_precision = max_scale + max_int_part;

            if (min_precision > DataTypeDecimal<Decimal256>::maxPrecision())
                throw Exception(
                    getExceptionMessagePrefix(types) + " because the least supertype is Decimal("
                        + toString(min_precision) + ',' + toString(max_scale) + ')',
                    ErrorCodes::NO_COMMON_TYPE);

            if (min_precision > DataTypeDecimal<Decimal128>::maxPrecision())
                return std::make_shared<DataTypeDecimal<Decimal256>>(min_precision, max_scale);
            if (min_precision > DataTypeDecimal<Decimal64>::maxPrecision())
                return std::make_shared<DataTypeDecimal<Decimal128>>(min_precision, max_scale);
            if (min_precision > DataTypeDecimal<Decimal32>::maxPrecision())
                return std::make_shared<DataTypeDecimal<Decimal64>>(min_precision, max_scale);
            return std::make_shared<DataTypeDecimal<Decimal32>>(min_precision, max_scale);
        }
    }

    /// For numeric types, the most complicated part.
    {
        bool all_numbers = true;

        size_t max_bits_of_signed_integer = 0;
        size_t max_bits_of_unsigned_integer = 0;
        size_t max_mantissa_bits_of_floating = 0;

        auto maximize = [](size_t & what, size_t value) {
            if (value > what)
                what = value;
        };

        for (const auto & type : types)
        {
            if (typeid_cast<const DataTypeUInt8 *>(type.get()))
                maximize(max_bits_of_unsigned_integer, 8);
            else if (typeid_cast<const DataTypeUInt16 *>(type.get()))
                maximize(max_bits_of_unsigned_integer, 16);
            else if (typeid_cast<const DataTypeUInt32 *>(type.get()))
                maximize(max_bits_of_unsigned_integer, 32);
            else if (typeid_cast<const DataTypeUInt64 *>(type.get()))
                maximize(max_bits_of_unsigned_integer, 64);
            else if (typeid_cast<const DataTypeInt8 *>(type.get()))
                maximize(max_bits_of_signed_integer, 8);
            else if (typeid_cast<const DataTypeInt16 *>(type.get()))
                maximize(max_bits_of_signed_integer, 16);
            else if (typeid_cast<const DataTypeInt32 *>(type.get()))
                maximize(max_bits_of_signed_integer, 32);
            else if (typeid_cast<const DataTypeInt64 *>(type.get()))
                maximize(max_bits_of_signed_integer, 64);
            else if (typeid_cast<const DataTypeFloat32 *>(type.get()))
                maximize(max_mantissa_bits_of_floating, 24);
            else if (typeid_cast<const DataTypeFloat64 *>(type.get()))
                maximize(max_mantissa_bits_of_floating, 53);
            else
                all_numbers = false;
        }

        if (max_bits_of_signed_integer || max_bits_of_unsigned_integer || max_mantissa_bits_of_floating)
        {
            if (!all_numbers)
                throw Exception(
                    getExceptionMessagePrefix(types) + " because some of them are numbers and some of them are not",
                    ErrorCodes::NO_COMMON_TYPE);

            /// If there are signed and unsigned types of same bit-width, the result must be signed number with at least one more bit.
            /// Example, common of Int32, UInt32 = Int64.

            size_t min_bit_width_of_integer = std::max(max_bits_of_signed_integer, max_bits_of_unsigned_integer);

            /// If there are both signed and unsigned types and the max unsigned width is 64, we will convert them to Decimal(20,0).
            /// Example, common of (Int64, UInt64) = Decimal(20,0)
            if (max_bits_of_signed_integer > 0 && max_bits_of_unsigned_integer == 64)
            {
                return std::make_shared<DataTypeDecimal<Decimal128>>(20, 0);
            }

            /// If unsigned is not covered by signed.
            if (max_bits_of_signed_integer && max_bits_of_unsigned_integer >= max_bits_of_signed_integer)
                ++min_bit_width_of_integer;

            /// If the result must be floating.
            if (max_mantissa_bits_of_floating)
            {
                size_t min_mantissa_bits = std::max(min_bit_width_of_integer, max_mantissa_bits_of_floating);
                if (min_mantissa_bits <= 24)
                    return std::make_shared<DataTypeFloat32>();
                else if (min_mantissa_bits <= 53)
                    return std::make_shared<DataTypeFloat64>();
                else
                    throw Exception(
                        getExceptionMessagePrefix(types)
                            + " because some of them are integers and some are floating point,"
                              " but there is no floating point type, that can exactly represent all required integers",
                        ErrorCodes::NO_COMMON_TYPE);
            }

            /// If the result must be signed integer.
            if (max_bits_of_signed_integer)
            {
                if (min_bit_width_of_integer <= 8)
                    return std::make_shared<DataTypeInt8>();
                else if (min_bit_width_of_integer <= 16)
                    return std::make_shared<DataTypeInt16>();
                else if (min_bit_width_of_integer <= 32)
                    return std::make_shared<DataTypeInt32>();
                else if (min_bit_width_of_integer <= 64)
                    return std::make_shared<DataTypeInt64>();
                else
                    throw Exception(
                        getExceptionMessagePrefix(types)
                            + " because some of them are signed integers and some are unsigned integers,"
                              " but there is no signed integer type, that can exactly represent all required unsigned "
                              "integer values",
                        ErrorCodes::NO_COMMON_TYPE);
            }

            /// All unsigned.
            {
                if (min_bit_width_of_integer <= 8)
                    return std::make_shared<DataTypeUInt8>();
                else if (min_bit_width_of_integer <= 16)
                    return std::make_shared<DataTypeUInt16>();
                else if (min_bit_width_of_integer <= 32)
                    return std::make_shared<DataTypeUInt32>();
                else if (min_bit_width_of_integer <= 64)
                    return std::make_shared<DataTypeUInt64>();
                else
                    throw Exception(
                        "Logical error: " + getExceptionMessagePrefix(types)
                            + " but as all data types are unsigned integers, we must have found maximum unsigned "
                              "integer type",
                        ErrorCodes::NO_COMMON_TYPE);
            }
        }
    }

    /// All other data types (UUID, AggregateFunction, Enum...) are compatible only if they are the same (checked in trivial cases).
    throw Exception(getExceptionMessagePrefix(types), ErrorCodes::NO_COMMON_TYPE);
}

} // namespace DB

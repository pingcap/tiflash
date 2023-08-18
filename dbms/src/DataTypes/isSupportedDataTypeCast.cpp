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
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/isSupportedDataTypeCast.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
bool isSupportedDataTypeCast(const DataTypePtr & from, const DataTypePtr & to)
{
    assert(from != nullptr && to != nullptr);
    /// `to` is equal to `from`
    if (to->equals(*from))
    {
        return true;
    }

    /// For Nullable, unwrap DataTypeNullable
    {
        bool has_nullable = false;
        DataTypePtr from_not_null;
        if (const auto * type_nullable = typeid_cast<const DataTypeNullable *>(from.get()))
        {
            has_nullable = true;
            from_not_null = type_nullable->getNestedType();
        }
        else
        {
            from_not_null = from;
        }

        DataTypePtr to_not_null;
        if (const auto * type_nullable = typeid_cast<const DataTypeNullable *>(to.get()))
        {
            has_nullable = true;
            to_not_null = type_nullable->getNestedType();
        }
        else
        {
            to_not_null = to;
        }

        if (has_nullable)
            return isSupportedDataTypeCast(from_not_null, to_not_null);
    }

    /// For numeric types (integer, floats)
    if (from->isNumber() && to->isNumber())
    {
        // float32 -> float64 is supported
        if (from->getTypeId() == TypeIndex::Float32 && to->getTypeId() == TypeIndex::Float64)
        {
            return true;
        }
        /// int <-> float, is not supported
        if (!from->isInteger() || !to->isInteger())
        {
            return false;
        }
        /// Change from signed to unsigned, or vice versa, is not supported
        // use xor(^)
        if ((from->isUnsignedInteger()) ^ (to->isUnsignedInteger()))
        {
            return false;
        }

        /// Both signed or unsigned, compare the sizeof(Type)
        size_t from_sz = from->getSizeOfValueInMemory();
        size_t to_sz = to->getSizeOfValueInMemory();
        return from_sz <= to_sz;
    }

    /// For String / FixedString
    if (from->isStringOrFixedString() && to->isStringOrFixedString())
    {
        size_t from_sz = std::numeric_limits<size_t>::max();
        if (const auto * type_fixed_str = typeid_cast<const DataTypeFixedString *>(from.get()))
            from_sz = type_fixed_str->getN();
        size_t to_sz = std::numeric_limits<size_t>::max();
        if (const auto * type_fixed_str = typeid_cast<const DataTypeFixedString *>(to.get()))
            to_sz = type_fixed_str->getN();
        return from_sz <= to_sz;
    }

    if (from->getTypeId() == TypeIndex::MyDateTime && to->getTypeId() == TypeIndex::MyDateTime)
    {
        const auto * const from_mydatetime = checkAndGetDataType<DataTypeMyDateTime>(from.get());
        const auto * const to_mydatetime = checkAndGetDataType<DataTypeMyDateTime>(to.get());
        // Enlarging the `fsp` of `mydatetime`/`timestamp`/`time` is a lossless change, TiFlash should detect and change the data type in place.
        // Narrowing down the `fsp` is a lossy change, TiDB will add a temporary column and reorganize the column data as other lossy type change.
        return (from_mydatetime->getFraction() < to_mydatetime->getFraction());
    }
    /// For other cases of Date and DateTime, not supported
    if (from->isDateOrDateTime() || to->isDateOrDateTime())
    {
        return false;
    }

    {
        bool from_is_decimal = IsDecimalDataType(from);
        bool to_is_decimal = IsDecimalDataType(to);
        if (from_is_decimal || to_is_decimal)
        {
            // not support change Decimal to other type, neither other type to Decimal
            return false;
        }
    }

    if (from->isEnum() && to->isEnum())
    {
        /// support cast Enum to Enum if the from type is a subset of the target type
        const auto * const from_enum8 = checkAndGetDataType<DataTypeEnum8>(from.get());
        const auto * const to_enum8 = checkAndGetDataType<DataTypeEnum8>(to.get());
        if (from_enum8 && to_enum8)
        {
            for (const auto & value : from_enum8->getValues())
            {
                if (!to_enum8->hasElement(value.first) || to_enum8->getValue(value.first) != value.second)
                    return false;
            }
        }
        const auto * const from_enum16 = checkAndGetDataType<DataTypeEnum16>(from.get());
        const auto * const to_enum16 = checkAndGetDataType<DataTypeEnum16>(to.get());
        if (from_enum16 && to_enum16)
        {
            for (const auto & value : from_enum16->getValues())
            {
                if (!to_enum16->hasElement(value.first) || to_enum16->getValue(value.first) != value.second)
                    return false;
            }
        }
        return true;
    }

    // TODO set?

    /// some DataTypes that support in ClickHouse but not in TiDB

    // Cast to Nothing / from Nothing is lossy
    if (typeid_cast<const DataTypeNothing *>(from.get()) || typeid_cast<const DataTypeNothing *>(to.get()))
    {
        return true;
    }

    // Cast to Array / from Array is not supported
    if (typeid_cast<const DataTypeArray *>(from.get()) || typeid_cast<const DataTypeArray *>(to.get()))
    {
        return false;
    }

    // Cast to Tuple / from Tuple is not supported
    if (typeid_cast<const DataTypeTuple *>(from.get()) || typeid_cast<const DataTypeTuple *>(to.get()))
    {
        return false;
    }

    return false;
}

} // namespace DB

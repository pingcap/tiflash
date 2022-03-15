// Copyright 2022 PingCAP, Ltd.
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

#include <ext/range.h>
#include <Dictionaries/ExternalResultDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}

void ExternalResultDescription::init(const Block & sample_block_)
{
    sample_block = sample_block_;

    const auto num_columns = sample_block.columns();
    types.reserve(num_columns);
    names.reserve(num_columns);
    sample_columns.reserve(num_columns);

    for (const auto idx : ext::range(0, num_columns))
    {
        const auto & column = sample_block.safeGetByPosition(idx);
        const auto type = column.type.get();

        if (typeid_cast<const DataTypeUInt8 *>(type))
            types.push_back(ValueType::UInt8);
        else if (typeid_cast<const DataTypeUInt16 *>(type))
            types.push_back(ValueType::UInt16);
        else if (typeid_cast<const DataTypeUInt32 *>(type))
            types.push_back(ValueType::UInt32);
        else if (typeid_cast<const DataTypeUInt64 *>(type))
            types.push_back(ValueType::UInt64);
        else if (typeid_cast<const DataTypeInt8 *>(type))
            types.push_back(ValueType::Int8);
        else if (typeid_cast<const DataTypeInt16 *>(type))
            types.push_back(ValueType::Int16);
        else if (typeid_cast<const DataTypeInt32 *>(type))
            types.push_back(ValueType::Int32);
        else if (typeid_cast<const DataTypeInt64 *>(type))
            types.push_back(ValueType::Int64);
        else if (typeid_cast<const DataTypeFloat32 *>(type))
            types.push_back(ValueType::Float32);
        else if (typeid_cast<const DataTypeFloat64 *>(type))
            types.push_back(ValueType::Float64);
        else if (typeid_cast<const DataTypeString *>(type))
            types.push_back(ValueType::String);
        else if (typeid_cast<const DataTypeDate *>(type))
            types.push_back(ValueType::Date);
        else if (typeid_cast<const DataTypeDateTime *>(type))
            types.push_back(ValueType::DateTime);
        else
            throw Exception{
                "Unsupported type " + type->getName(),
                ErrorCodes::UNKNOWN_TYPE};

        names.emplace_back(column.name);
        sample_columns.emplace_back(column.column);

        /// If default value for column was not provided, use default from data type.
        if (sample_columns.back()->empty())
        {
            MutableColumnPtr mutable_column = (*std::move(sample_columns.back())).mutate();
            column.type->insertDefaultInto(*mutable_column);
            sample_columns.back() = std::move(mutable_column);
        }
    }
}

}

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

#include <Storages/DeltaMerge/workload/Utils.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

namespace DB::DM::tests
{
std::string localTime()
{
    return fmt::format("{:%Y%m%d%H%M%S}", fmt::localtime(time(nullptr)));
}

std::string localDate()
{
    return fmt::format("{:%Y%m%d}", fmt::localtime(::time(nullptr)));
}

std::string fieldToString(const DataTypePtr & data_type, const Field & f)
{
    std::string family_name = data_type->getFamilyName();
    if (family_name == "Int8" || family_name == "Int16" || family_name == "Int32" || family_name == "Int64"
        || family_name == "Enum8" || family_name == "Enum16")
    {
        auto i = f.safeGet<Int64>();
        return std::to_string(i);
    }
    else if (family_name == "UInt8" || family_name == "UInt16" || family_name == "UInt32" || family_name == "UInt64")
    {
        auto i = f.safeGet<UInt64>();
        return std::to_string(i);
    }
    else if (family_name == "Float32" || family_name == "Float64")
    {
        auto i = f.safeGet<Float64>();
        return std::to_string(i);
    }
    else if (family_name == "String")
    {
        return f.toString();
    }
    else if (family_name == "MyDateTime")
    {
        auto i = f.safeGet<UInt64>();
        return MyDateTime(i).toString(6);
    }
    else if (family_name == "MyDate")
    {
        auto i = f.safeGet<UInt64>();
        return MyDate(i).toString();
    }
    else if (family_name == "Decimal")
    {
        auto t = f.getType();
        if (t == Field::Types::Which::Decimal32)
        {
            auto i = f.get<Decimal32>();
            auto scale = dynamic_cast<const DataTypeDecimal32 *>(data_type.get())->getScale();
            return i.toString(scale);
        }
        else if (t == Field::Types::Which::Decimal64)
        {
            auto i = f.get<Decimal64>();
            auto scale = dynamic_cast<const DataTypeDecimal64 *>(data_type.get())->getScale();
            return i.toString(scale);
        }
        else if (t == Field::Types::Which::Decimal128)
        {
            auto i = f.get<Decimal128>();
            auto scale = dynamic_cast<const DataTypeDecimal128 *>(data_type.get())->getScale();
            return i.toString(scale);
        }
        else if (t == Field::Types::Which::Decimal256)
        {
            const auto & i = f.get<Decimal256>();
            auto scale = dynamic_cast<const DataTypeDecimal256 *>(data_type.get())->getScale();
            return i.toString(scale);
        }
    }
    return f.toString();
}

std::vector<std::string> colToVec(const DataTypePtr & data_type, const ColumnPtr & col)
{
    std::vector<std::string> v;
    for (size_t i = 0; i < col->size(); i++)
    {
        const Field & f = (*col)[i];
        v.push_back(fieldToString(data_type, f));
    }
    return v;
}

std::string blockToString(const Block & block)
{
    std::string s = "id name type values\n";
    const auto & cols = block.getColumnsWithTypeAndName();
    for (const auto & col : cols)
    {
        s += fmt::format(
            "{} {} {} {}\n",
            col.column_id,
            col.name,
            col.type->getFamilyName(),
            colToVec(col.type, col.column));
    }
    return s;
}

} // namespace DB::DM::tests

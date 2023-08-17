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

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsTiDBConversion.h>

namespace DB
{
String trim(const StringRef & value)
{
    StringRef ret;
    ret.size = 0;
    size_t start = 0;
    static std::unordered_set<char> spaces{'\t', '\n', '\v', '\f', '\r', ' '};
    for (; start < value.size; start++)
    {
        if (!spaces.count(value.data[start]))
            break;
    }
    size_t end = value.size;
    for (; start < end; end--)
    {
        if (!spaces.count(value.data[end - 1]))
            break;
    }
    if (start >= end)
        return ret.toString();
    ret.data = value.data + start;
    ret.size = end - start;
    return ret.toString();
}

void registerFunctionsTiDBConversion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBuilderTiDBCast>();
}

FunctionBasePtr FunctionBuilderTiDBCast::buildImpl(
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & return_type,
    const TiDB::TiDBCollatorPtr &) const
{
    DataTypes data_types(arguments.size());

    for (size_t i = 0; i < arguments.size(); ++i)
        data_types[i] = arguments[i].type;

    auto monotonicity = getMonotonicityInformation(arguments.front().type, return_type.get());
    return std::make_shared<FunctionTiDBCast<>>(
        context,
        name,
        std::move(monotonicity),
        data_types,
        return_type,
        in_union,
        tidb_tp);
}


} // namespace DB

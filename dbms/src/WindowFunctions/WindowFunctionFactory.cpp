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

#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/WriteHelpers.h>
#include <WindowFunctions/WindowFunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_WINDOW_FUNCTION;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

void WindowFunctionFactory::registerFunction(const String & name, Creator creator)
{
    if (creator == nullptr)
        throw Exception(
            fmt::format("WindowFunctionFactory: the window function {} has been provided a null constructor", name),
            ErrorCodes::LOGICAL_ERROR);

    if (!window_functions.emplace(name, creator).second)
        throw Exception(
            fmt::format("WindowFunctionFactory: the window function {} is not unique", name),
            ErrorCodes::LOGICAL_ERROR);
}

WindowFunctionPtr WindowFunctionFactory::get(const String & name, const DataTypes & argument_types) const
{
    auto res = getImpl(name, argument_types);
    if (!res)
        throw Exception("Logical error: WindowFunctionFactory returned nullptr", ErrorCodes::LOGICAL_ERROR);
    return res;
}


WindowFunctionPtr WindowFunctionFactory::getImpl(const String & name, const DataTypes & argument_types) const
{
    /// Find by exact match.
    auto it = window_functions.find(name);
    if (it != window_functions.end())
        return it->second(argument_types);

    throw Exception("Unknown window function " + name, ErrorCodes::UNKNOWN_WINDOW_FUNCTION);
}


WindowFunctionPtr WindowFunctionFactory::tryGet(const String & name, const DataTypes & argument_types) const
{
    return isWindowFunctionName(name) ? get(name, argument_types) : nullptr;
}


bool WindowFunctionFactory::isWindowFunctionName(const String & name) const
{
    return window_functions.count(name);
}

} // namespace DB

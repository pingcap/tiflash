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

#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Poco/String.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_FUNCTION;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes


void FunctionFactory::registerFunction(const std::string & name, Creator creator, CaseSensitiveness case_sensitiveness)
{
    if (!functions.emplace(name, creator).second)
        throw Exception("FunctionFactory: the function name '" + name + "' is not unique", ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_functions.emplace(Poco::toLower(name), creator).second)
        throw Exception(
            "FunctionFactory: the case insensitive function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}


FunctionBuilderPtr FunctionFactory::get(const std::string & name, const Context & context) const
{
    auto res = tryGet(name, context);
    if (!res)
        throw Exception("Unknown function " + name, ErrorCodes::UNKNOWN_FUNCTION);
    return res;
}


FunctionBuilderPtr FunctionFactory::tryGet(const std::string & name, const Context & context) const
{
    auto it = functions.find(name);
    if (functions.end() != it)
        return it->second(context);

    it = case_insensitive_functions.find(Poco::toLower(name));
    if (case_insensitive_functions.end() != it)
        return it->second(context);

    return {};
}

} // namespace DB

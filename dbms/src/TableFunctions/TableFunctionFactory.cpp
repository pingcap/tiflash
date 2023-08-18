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
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
extern const int READONLY;
extern const int UNKNOWN_FUNCTION;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes


void TableFunctionFactory::registerFunction(const std::string & name, Creator creator)
{
    if (!functions.emplace(name, std::move(creator)).second)
        throw Exception(
            "TableFunctionFactory: the table function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}

TableFunctionPtr TableFunctionFactory::get(const std::string & name, const Context & context) const
{
    if (context.getSettingsRef().readonly == 1) /** For example, for readonly = 2 - allowed. */
        throw Exception("Table functions are forbidden in readonly mode", ErrorCodes::READONLY);

    auto it = functions.find(name);
    if (it == functions.end())
        throw Exception("Unknown table function " + name, ErrorCodes::UNKNOWN_FUNCTION);

    return it->second();
}

} // namespace DB

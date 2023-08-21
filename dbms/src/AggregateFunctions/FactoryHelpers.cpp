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

#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
namespace ErrorCodes
{
extern const int AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

void assertNoParameters(const std::string & name, const Array & parameters)
{
    if (!parameters.empty())
        throw Exception(
            "Aggregate function " + name + " cannot have parameters",
            ErrorCodes::AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS);
}

void assertUnary(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 1)
        throw Exception(
            "Aggregate function " + name + " require single argument",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

void assertBinary(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 2)
        throw Exception(
            "Aggregate function " + name + " require two arguments",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

} // namespace DB

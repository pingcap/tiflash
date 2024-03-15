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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSequenceMatch.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
AggregateFunctionPtr createAggregateFunctionSequenceCount(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & params)
{
    if (params.size() != 1)
        throw Exception{
            "Aggregate function " + name + " requires exactly one parameter.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    String pattern = params.front().safeGet<std::string>();
    return std::make_shared<AggregateFunctionSequenceCount>(argument_types, pattern);
}

AggregateFunctionPtr createAggregateFunctionSequenceMatch(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & params)
{
    if (params.size() != 1)
        throw Exception{
            "Aggregate function " + name + " requires exactly one parameter.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    String pattern = params.front().safeGet<std::string>();
    return std::make_shared<AggregateFunctionSequenceMatch>(argument_types, pattern);
}

} // namespace

void registerAggregateFunctionsSequenceMatch(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sequenceMatch", createAggregateFunctionSequenceMatch);
    factory.registerFunction("sequenceCount", createAggregateFunctionSequenceCount);
}

} // namespace DB

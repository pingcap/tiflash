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
#include <AggregateFunctions/AggregateFunctionSumMap.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace
{
AggregateFunctionPtr createAggregateFunctionSumMap(
    const std::string & name,
    const DataTypes & arguments,
    const Array & params)
{
    assertNoParameters(name, params);

    if (arguments.size() < 2)
        throw Exception(
            "Aggregate function " + name + " requires at least two arguments of Array type.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
        throw Exception(
            "First argument for function " + name + " must be an array.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    const DataTypePtr & keys_type = array_type->getNestedType();

    DataTypes values_types;
    for (size_t i = 1; i < arguments.size(); ++i)
    {
        array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
        if (!array_type)
            throw Exception(
                "Argument #" + toString(i) + " for function " + name + " must be an array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        values_types.push_back(array_type->getNestedType());
    }

    AggregateFunctionPtr res(
        createWithNumericType<AggregateFunctionSumMap>(*keys_type, keys_type, std::move(values_types)));
    if (!res)
        throw Exception(
            "Illegal type of argument for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

} // namespace

void registerAggregateFunctionSumMap(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sumMap", createAggregateFunctionSumMap);
}

} // namespace DB

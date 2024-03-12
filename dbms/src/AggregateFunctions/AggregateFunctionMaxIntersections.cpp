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
#include <AggregateFunctions/AggregateFunctionMaxIntersections.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace DB
{
namespace
{
AggregateFunctionPtr createAggregateFunctionMaxIntersections(
    AggregateFunctionIntersectionsKind kind,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    assertBinary(name, argument_types);
    assertNoParameters(name, parameters);

    AggregateFunctionPtr res(
        createWithNumericType<AggregateFunctionIntersectionsMax>(*argument_types[0], kind, argument_types));
    if (!res)
        throw Exception(
            "Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
                + " of argument for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}
} // namespace

void registerAggregateFunctionsMaxIntersections(AggregateFunctionFactory & factory)
{
    factory.registerFunction(
        "maxIntersections",
        [](const Context & /* context not used */,
           const std::string & name,
           const DataTypes & argument_types,
           const Array & parameters) {
            return createAggregateFunctionMaxIntersections(
                AggregateFunctionIntersectionsKind::Count,
                name,
                argument_types,
                parameters);
        });

    factory.registerFunction(
        "maxIntersectionsPosition",
        [](const Context & /* context not used */,
           const std::string & name,
           const DataTypes & argument_types,
           const Array & parameters) {
            return createAggregateFunctionMaxIntersections(
                AggregateFunctionIntersectionsKind::Position,
                name,
                argument_types,
                parameters);
        });
}

} // namespace DB

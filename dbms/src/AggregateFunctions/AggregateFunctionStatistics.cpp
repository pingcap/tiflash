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
#include <AggregateFunctions/AggregateFunctionStatistics.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
template <template <typename> class FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsUnary(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<FunctionTemplate>(*argument_types[0]));

    if (!res)
        throw Exception(
            "Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

template <template <typename, typename> class FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsBinary(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoNumericTypes<FunctionTemplate>(*argument_types[0], *argument_types[1]));
    if (!res)
        throw Exception(
            "Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
                + " of arguments for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

} // namespace

void registerAggregateFunctionsStatisticsStable(AggregateFunctionFactory & factory)
{
    factory.registerFunction("varSampStable", createAggregateFunctionStatisticsUnary<AggregateFunctionVarSampStable>);
    factory.registerFunction("varPopStable", createAggregateFunctionStatisticsUnary<AggregateFunctionVarPopStable>);
    factory.registerFunction(
        "stddevSampStable",
        createAggregateFunctionStatisticsUnary<AggregateFunctionStddevSampStable>);
    factory.registerFunction(
        "stddevPopStable",
        createAggregateFunctionStatisticsUnary<AggregateFunctionStddevPopStable>);
    factory.registerFunction(
        "covarSampStable",
        createAggregateFunctionStatisticsBinary<AggregateFunctionCovarSampStable>);
    factory.registerFunction(
        "covarPopStable",
        createAggregateFunctionStatisticsBinary<AggregateFunctionCovarPopStable>);
    factory.registerFunction("corrStable", createAggregateFunctionStatisticsBinary<AggregateFunctionCorrStable>);
}

} // namespace DB

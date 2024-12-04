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
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>


namespace DB
{
namespace
{
AggregateFunctionPtr createAggregateFunctionAny(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    return AggregateFunctionPtr(
        createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyData>(
            name,
            argument_types,
            parameters));
}

AggregateFunctionPtr createAggregateFunctionFirstRow(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    return AggregateFunctionPtr(
        createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionFirstRowData>(
            name,
            argument_types,
            parameters));
}

AggregateFunctionPtr createAggregateFunctionAnyLast(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    return AggregateFunctionPtr(
        createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyLastData>(
            name,
            argument_types,
            parameters));
}

AggregateFunctionPtr createAggregateFunctionAnyHeavy(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    return AggregateFunctionPtr(
        createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyHeavyData>(
            name,
            argument_types,
            parameters));
}

AggregateFunctionPtr createAggregateFunctionMin(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    return AggregateFunctionPtr(
        createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMinData>(
            name,
            argument_types,
            parameters));
}

AggregateFunctionPtr createAggregateFunctionMax(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    return AggregateFunctionPtr(
        createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMaxData>(
            name,
            argument_types,
            parameters));
}

AggregateFunctionPtr createAggregateFunctionArgMin(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    return AggregateFunctionPtr(
        createAggregateFunctionArgMinMax<AggregateFunctionMinData>(name, argument_types, parameters));
}

AggregateFunctionPtr createAggregateFunctionArgMax(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    return AggregateFunctionPtr(
        createAggregateFunctionArgMinMax<AggregateFunctionMaxData>(name, argument_types, parameters));
}

} // namespace

void registerAggregateFunctionsMinMaxAny(AggregateFunctionFactory & factory)
{
    factory.registerFunction("any", createAggregateFunctionAny); // TODO no use
    factory.registerFunction("first_row", createAggregateFunctionFirstRow);
    factory.registerFunction("anyLast", createAggregateFunctionAnyLast); // TODO no use
    factory.registerFunction("anyHeavy", createAggregateFunctionAnyHeavy); // TODO no use
    factory.registerFunction("min", createAggregateFunctionMin, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("max", createAggregateFunctionMax, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("argMin", createAggregateFunctionArgMin); // TODO no use
    factory.registerFunction("argMax", createAggregateFunctionArgMax); // TODO no use
}

} // namespace DB

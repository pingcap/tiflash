// Copyright 2022 PingCAP, Ltd.
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
#include <AggregateFunctions/AggregateFunctionGroupArrayInsertAt.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace DB
{
namespace
{
AggregateFunctionPtr createAggregateFunctionGroupArrayInsertAt(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertBinary(name, argument_types);
    return std::make_shared<AggregateFunctionGroupArrayInsertAtGeneric>(argument_types, parameters);
}

} // namespace

void registerAggregateFunctionGroupArrayInsertAt(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupArrayInsertAt", createAggregateFunctionGroupArrayInsertAt);
}

} // namespace DB

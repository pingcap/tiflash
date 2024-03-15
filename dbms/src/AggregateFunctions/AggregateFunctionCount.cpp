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

#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
namespace
{
AggregateFunctionPtr createAggregateFunctionCount(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & /*argument_types*/,
    const Array & parameters)
{
    assertNoParameters(name, parameters);

    /// 'count' accept any number of arguments and (in this case of non-Nullable types) simply ignore them.
    return std::make_shared<AggregateFunctionCount>();
}

} // namespace

void registerAggregateFunctionCount(AggregateFunctionFactory & factory)
{
    factory.registerFunction("count", createAggregateFunctionCount, AggregateFunctionFactory::CaseInsensitive);
}

} // namespace DB

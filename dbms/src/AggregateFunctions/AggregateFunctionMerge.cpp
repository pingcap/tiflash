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

#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/AggregateFunctionMerge.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

class AggregateFunctionCombinatorMerge final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Merge"; };

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                "Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypePtr & argument = arguments[0];

        const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(argument.get());
        if (!function)
            throw Exception(
                "Illegal type " + argument->getName() + " of argument for aggregate function with " + getName()
                    + " suffix" + " must be AggregateFunction(...)",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return function->getArgumentsDataTypes();
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array &) const override
    {
        const DataTypePtr & argument = arguments[0];

        const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(argument.get());
        if (!function)
            throw Exception(
                "Illegal type " + argument->getName() + " of argument for aggregate function with " + getName()
                    + " suffix" + " must be AggregateFunction(...)",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (nested_function->getName() != function->getFunctionName())
            throw Exception(
                "Illegal type " + argument->getName() + " of argument for aggregate function with " + getName()
                    + " suffix" + ", because it corresponds to different aggregate function: "
                    + function->getFunctionName() + " instead of " + nested_function->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<AggregateFunctionMerge>(nested_function, *argument);
    }
};

void registerAggregateFunctionCombinatorMerge(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorMerge>());
}

} // namespace DB

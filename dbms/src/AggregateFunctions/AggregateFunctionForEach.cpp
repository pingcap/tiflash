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
#include <AggregateFunctions/AggregateFunctionForEach.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class AggregateFunctionCombinatorForEach final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "ForEach"; };

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        DataTypes nested_arguments;
        for (const auto & type : arguments)
        {
            if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(type.get()))
                nested_arguments.push_back(array->getNestedType());
            else
                throw Exception(
                    "Illegal type " + type->getName()
                        + " of argument"
                          " for aggregate function with "
                        + getName() + " suffix. Must be array.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return nested_arguments;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array &) const override
    {
        return std::make_shared<AggregateFunctionForEach>(nested_function, arguments);
    }
};

void registerAggregateFunctionCombinatorForEach(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorForEach>());
}

} // namespace DB

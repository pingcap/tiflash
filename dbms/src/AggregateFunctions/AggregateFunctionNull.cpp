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
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/AggregateFunctionNull.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>

#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

extern const std::unordered_set<String> hacking_return_non_null_agg_func_names;

class AggregateFunctionCombinatorNull final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Null"; };

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        size_t size = arguments.size();
        DataTypes res(size);
        for (size_t i = 0; i < size; ++i)
            res[i] = removeNullable(arguments[i]);
        return res;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array &) const override
    {
        /// group_concat reuses groupArray and groupUniqArray with the special warp function `AggregateFunctionGroupConcat` to process,
        /// the warp function needs more complex arguments, including collators, sort descriptions and others, which are hard to deliver via Array type,
        /// so it is specially added outside, instead of being added here, so directly return in this function.
        if (nested_function
            && (nested_function->getName() == "groupArray" || nested_function->getName() == "groupUniqArray"))
            return nested_function;
        bool has_nullable_types = false;
        bool has_null_types = false;
        for (const auto & arg_type : arguments)
        {
            if (arg_type->isNullable())
            {
                has_nullable_types = true;
                if (arg_type->onlyNull())
                {
                    has_null_types = true;
                    break;
                }
            }
        }

        /// Special case for 'count' function. It could be called with Nullable arguments
        /// - that means - count number of calls, when all arguments are not NULL.
        if (nested_function && nested_function->getName() == "count")
        {
            if (has_nullable_types)
            {
                if (arguments.size() == 1)
                    return std::make_shared<AggregateFunctionCountNotNullUnary>(arguments[0]);
                else
                    return std::make_shared<AggregateFunctionCountNotNullVariadic>(arguments);
            }
            else
            {
                return std::make_shared<AggregateFunctionCount>();
            }
        }

        bool can_output_be_null = true;
        if (nested_function && hacking_return_non_null_agg_func_names.count(nested_function->getName()))
            can_output_be_null = false;

        if (has_null_types && can_output_be_null)
            return std::make_shared<AggregateFunctionNothing>();

        bool return_type_is_nullable = can_output_be_null && nested_function->getReturnType()->canBeInsideNullable();

        if (nested_function && nested_function->getName() == "first_row")
        {
            if (return_type_is_nullable)
            {
                if (has_nullable_types)
                    return std::make_shared<AggregateFunctionFirstRowNull<true, true>>(nested_function);
                else
                    return std::make_shared<AggregateFunctionFirstRowNull<true, false>>(nested_function);
            }
            else
            {
                if (has_nullable_types)
                    return std::make_shared<AggregateFunctionFirstRowNull<false, true>>(nested_function);
                else
                    return std::make_shared<AggregateFunctionFirstRowNull<false, false>>(nested_function);
            }
        }

        if (arguments.size() == 1)
        {
            if (return_type_is_nullable)
            {
                if (has_nullable_types)
                    return std::make_shared<AggregateFunctionNullUnary<true, true>>(nested_function);
                else
                    return std::make_shared<AggregateFunctionNullUnary<true, false>>(nested_function);
            }
            else
            {
                if (has_nullable_types)
                    return std::make_shared<AggregateFunctionNullUnary<false, true>>(nested_function);
                else
                    return std::make_shared<AggregateFunctionNullUnary<false, false>>(nested_function);
            }
        }
        else
        {
            if (return_type_is_nullable)
                return std::make_shared<AggregateFunctionNullVariadic<true>>(nested_function, arguments);
            else
                return std::make_shared<AggregateFunctionNullVariadic<false>>(nested_function, arguments);
        }
    }
};

void registerAggregateFunctionCombinatorNull(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorNull>());
}

} // namespace DB

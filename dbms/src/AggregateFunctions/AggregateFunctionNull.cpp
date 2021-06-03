#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/AggregateFunctionNull.h>
#include <DataTypes/DataTypeNullable.h>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

extern const String UniqRawResName;
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
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array &) const override
    {
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

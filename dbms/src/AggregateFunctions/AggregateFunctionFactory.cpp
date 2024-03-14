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
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Buffer/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/String.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_AGGREGATE_FUNCTION;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

extern const String uniq_raw_res_name = "uniqRawRes";
extern const String count_second_stage;

void AggregateFunctionFactory::registerFunction(
    const String & name,
    Creator creator,
    CaseSensitiveness case_sensitiveness)
{
    if (creator == nullptr)
        throw Exception(
            "AggregateFunctionFactory: the aggregate function " + name + " has been provided a null constructor",
            ErrorCodes::LOGICAL_ERROR);

    if (!aggregate_functions.emplace(name, creator).second)
        throw Exception(
            "AggregateFunctionFactory: the aggregate function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_aggregate_functions.emplace(Poco::toLower(name), creator).second)
        throw Exception(
            "AggregateFunctionFactory: the case insensitive aggregate function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}

/// A little hack - if we have NULL arguments, don't even create nested function.
/// Combinator will check if nested_function was created.
/// TODO Consider replace with function property. See also https://github.com/ClickHouse/ClickHouse/pull/11661
extern const std::unordered_set<String> hacking_return_non_null_agg_func_names
    = {"count", "uniq", "uniqHLL12", "uniqExact", "uniqCombined", uniq_raw_res_name, count_second_stage};

AggregateFunctionPtr AggregateFunctionFactory::get(
    const Context & context,
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    int recursion_level,
    bool empty_input_as_null) const
{
    /// If one of types is Nullable, we apply aggregate function combinator "Null".

    /// for most aggregation functions except `count`, if the input is empty, the function should return NULL
    /// so add this flag to make it possible to follow this rule, currently only used by Coprocessor query
    if (empty_input_as_null || std::any_of(argument_types.begin(), argument_types.end(), [](const auto & type) {
            return type->isNullable();
        }))
    {
        AggregateFunctionCombinatorPtr combinator
            = AggregateFunctionCombinatorFactory::instance().tryFindSuffix("Null");
        if (!combinator)
            throw Exception(
                "Logical error: cannot find aggregate function combinator to apply a function to Nullable arguments.",
                ErrorCodes::LOGICAL_ERROR);

        DataTypes nested_types = combinator->transformArguments(argument_types);

        AggregateFunctionPtr nested_function;

        if (hacking_return_non_null_agg_func_names.count(name)
            || std::none_of(argument_types.begin(), argument_types.end(), [](const auto & type) {
                   return type->onlyNull();
               }))
            nested_function = getImpl(context, name, nested_types, parameters, recursion_level);

        return combinator->transformAggregateFunction(nested_function, argument_types, parameters);
    }

    auto res = getImpl(context, name, argument_types, parameters, recursion_level);
    if (!res)
        throw Exception("Logical error: AggregateFunctionFactory returned nullptr", ErrorCodes::LOGICAL_ERROR);
    return res;
}

AggregateFunctionPtr AggregateFunctionFactory::getImpl(
    const Context & context,
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    int recursion_level) const
{
    /// Find by exact match.
    auto it = aggregate_functions.find(name);
    if (it != aggregate_functions.end())
        return it->second(context, name, argument_types, parameters);

    /// Find by case-insensitive name.
    /// Combinators cannot apply for case insensitive (SQL-style) aggregate function names. Only for native names.
    if (recursion_level == 0)
    {
        auto it = case_insensitive_aggregate_functions.find(Poco::toLower(name));
        if (it != case_insensitive_aggregate_functions.end())
            return it->second(context, name, argument_types, parameters);
    }

    /// Combinators of aggregate functions.
    /// For every aggregate function 'agg' and combiner '-Comb' there is combined aggregate function with name 'aggComb',
    ///  that can have different number and/or types of arguments, different result type and different behaviour.

    if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
    {
        if (combinator->getName() == "Null")
            throw Exception(
                "Aggregate function combinator 'Null' is only for internal usage",
                ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);

        String nested_name = name.substr(0, name.size() - combinator->getName().size());
        DataTypes nested_types = combinator->transformArguments(argument_types);
        AggregateFunctionPtr nested_function
            = getImpl(context, nested_name, nested_types, parameters, recursion_level + 1);
        return combinator->transformAggregateFunction(nested_function, argument_types, parameters);
    }

    throw Exception("Unknown aggregate function " + name, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
}

AggregateFunctionPtr AggregateFunctionFactory::tryGet(
    const Context & context,
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters) const
{
    return isAggregateFunctionName(name) ? get(context, name, argument_types, parameters) : nullptr;
}


bool AggregateFunctionFactory::isAggregateFunctionName(const String & name, int recursion_level) const
{
    if (aggregate_functions.count(name))
        return true;

    if (recursion_level == 0 && case_insensitive_aggregate_functions.count(Poco::toLower(name)))
        return true;

    if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
        return isAggregateFunctionName(name.substr(0, name.size() - combinator->getName().size()), recursion_level + 1);

    return false;
}

} // namespace DB

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
#include <AggregateFunctions/AggregateFunctionUniqUpTo.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <fmt/core.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ARGUMENT_OUT_OF_BOUND;
} // namespace ErrorCodes


namespace
{
constexpr UInt8 uniq_upto_max_threshold = 100;


AggregateFunctionPtr createAggregateFunctionUniqUpTo(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & params)
{
    UInt8 threshold = 5; /// default value

    if (!params.empty())
    {
        if (params.size() != 1)
            throw Exception(
                fmt::format("Aggregate function {} requires one parameter or less.", name),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 threshold_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (threshold_param > uniq_upto_max_threshold)
            throw Exception(
                fmt::format(
                    "Too large parameter for aggregate function {}. Maximum: {}",
                    name,
                    uniq_upto_max_threshold),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        threshold = threshold_param;
    }

    if (argument_types.empty())
        throw Exception(
            fmt::format("Incorrect number of arguments for aggregate function {}", name),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniqUpTo>(*argument_types[0], threshold));

        if (res)
            return res;
        else if (typeid_cast<const DataTypeDate *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpTo<DataTypeDate::FieldType>>(threshold);
        else if (typeid_cast<const DataTypeDateTime *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpTo<DataTypeDateTime::FieldType>>(threshold);
        else if (
            typeid_cast<const DataTypeString *>(&argument_type)
            || typeid_cast<const DataTypeFixedString *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpTo<String>>(threshold);
        else if (typeid_cast<const DataTypeTuple *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpToVariadic<true>>(argument_types, threshold);
        else if (typeid_cast<const DataTypeUUID *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpTo<DataTypeUUID::FieldType>>(threshold);
    }
    else
    {
        /// If there are several arguments, then no tuples allowed among them.
        for (const auto & type : argument_types)
            if (typeid_cast<const DataTypeTuple *>(type.get()))
                throw Exception(
                    fmt::format("Tuple argument of function {} must be the only argument", name),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    return std::make_shared<AggregateFunctionUniqUpToVariadic<false>>(argument_types, threshold);
}

} // namespace

void registerAggregateFunctionUniqUpTo(AggregateFunctionFactory & factory)
{
    factory.registerFunction("uniqUpTo", createAggregateFunctionUniqUpTo);
}

} // namespace DB

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
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes


namespace
{
/** `DataForVariadic` is a data structure that will be used for `uniq` aggregate function of multiple arguments.
  * It differs, for example, in that it uses a trivial hash function, since `uniq` of many arguments first hashes them out itself.
  */

template <typename Data, typename DataForVariadic>
AggregateFunctionPtr createAggregateFunctionUniq(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & params)
{
    assertNoParameters(name, params);

    if (argument_types.empty())
        throw Exception(
            "Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data>(*argument_types[0]));

        if (res)
            return res;
        else if (typeid_cast<const DataTypeDate *>(&argument_type))
            return std::make_shared<AggregateFunctionUniq<DataTypeDate::FieldType, Data>>();
        else if (typeid_cast<const DataTypeDateTime *>(&argument_type))
            return std::make_shared<AggregateFunctionUniq<DataTypeDateTime::FieldType, Data>>();
        else if (
            typeid_cast<const DataTypeString *>(&argument_type)
            || typeid_cast<const DataTypeFixedString *>(&argument_type))
            return std::make_shared<AggregateFunctionUniq<String, Data>>();
        else if (typeid_cast<const DataTypeTuple *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, true>>(argument_types);
        else if (typeid_cast<const DataTypeUUID *>(&argument_type))
            return std::make_shared<AggregateFunctionUniq<DataTypeUUID::FieldType, Data>>();
    }
    else
    {
        /// If there are several arguments, then no tuples allowed among them.
        for (const auto & type : argument_types)
            if (typeid_cast<const DataTypeTuple *>(type.get()))
                throw Exception(
                    "Tuple argument of function " + name + " must be the only argument",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, false>>(argument_types);
}

template <template <typename> class Data, typename DataForVariadic>
AggregateFunctionPtr createAggregateFunctionUniq(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & params)
{
    assertNoParameters(name, params);

    if (argument_types.empty())
        throw Exception(
            "Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data>(*argument_types[0]));

        if (res)
            return res;
        else if (typeid_cast<const DataTypeDate *>(&argument_type))
            return std::make_shared<AggregateFunctionUniq<DataTypeDate::FieldType, Data<DataTypeDate::FieldType>>>();
        else if (typeid_cast<const DataTypeDateTime *>(&argument_type))
            return std::make_shared<
                AggregateFunctionUniq<DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType>>>();
        else if (
            typeid_cast<const DataTypeString *>(&argument_type)
            || typeid_cast<const DataTypeFixedString *>(&argument_type))
            return std::make_shared<AggregateFunctionUniq<String, Data<String>>>();
        else if (typeid_cast<const DataTypeTuple *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, true>>(argument_types);
        else if (typeid_cast<const DataTypeUUID *>(&argument_type))
            return std::make_shared<AggregateFunctionUniq<DataTypeUUID::FieldType, Data<DataTypeUUID::FieldType>>>();
    }
    else
    {
        /// If there are several arguments, then no tuples allowed among them.
        for (const auto & type : argument_types)
            if (typeid_cast<const DataTypeTuple *>(type.get()))
                throw Exception(
                    "Tuple argument of function " + name + " must be the only argument",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, false>>(argument_types);
}

AggregateFunctionPtr createAggregateFunctionUniqRawRes(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & params)
{
    assertNoParameters(name, params);

    if (argument_types.empty())
        throw Exception(
            "Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// If there are several arguments, then no tuples allowed among them.
    for (const auto & type : argument_types)
        if (typeid_cast<const DataTypeTuple *>(type.get()))
            throw Exception(
                "Tuple argument of function " + name + " must be the only argument",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    /// "Variadic" method also works as a fallback generic case for single argument.
    return std::make_shared<
        AggregateFunctionUniqVariadic<AggregateFunctionUniqUniquesHashSetDataForVariadicRawRes, false, true>>(
        argument_types);
}

} // namespace

void registerAggregateFunctionsUniq(AggregateFunctionFactory & factory)
{
    factory.registerFunction(
        "uniq",
        createAggregateFunctionUniq<
            AggregateFunctionUniqUniquesHashSetData,
            AggregateFunctionUniqUniquesHashSetDataForVariadic>);

    factory.registerFunction(
        "uniqHLL12",
        createAggregateFunctionUniq<AggregateFunctionUniqHLL12Data, AggregateFunctionUniqHLL12DataForVariadic>);

    factory.registerFunction(
        "uniqExact",
        createAggregateFunctionUniq<AggregateFunctionUniqExactData, AggregateFunctionUniqExactData<String>>);

    factory.registerFunction(
        "uniqCombined",
        createAggregateFunctionUniq<AggregateFunctionUniqCombinedData, AggregateFunctionUniqCombinedData<UInt64>>);

    factory.registerFunction(uniq_raw_res_name, createAggregateFunctionUniqRawRes);
}

} // namespace DB

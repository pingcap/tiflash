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
#include <AggregateFunctions/AggregateFunctionTopK.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <fmt/core.h>

#define TOP_K_MAX_SIZE 0xFFFFFF


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ARGUMENT_OUT_OF_BOUND;
} // namespace ErrorCodes


namespace
{
/// Substitute return type for Date and DateTime
class AggregateFunctionTopKDate : public AggregateFunctionTopK<DataTypeDate::FieldType>
{
    using AggregateFunctionTopK<DataTypeDate::FieldType>::AggregateFunctionTopK;
    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>());
    }
};

class AggregateFunctionTopKDateTime : public AggregateFunctionTopK<DataTypeDateTime::FieldType>
{
    using AggregateFunctionTopK<DataTypeDateTime::FieldType>::AggregateFunctionTopK;
    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>());
    }
};


IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, UInt64 threshold)
{
    if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return new AggregateFunctionTopKDate(threshold);
    if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return new AggregateFunctionTopKDateTime(threshold);

    /// Check that we can use plain version of AggregateFunctionTopKGeneric
    if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        return new AggregateFunctionTopKGeneric<true>(threshold, argument_type);
    else
        return new AggregateFunctionTopKGeneric<false>(threshold, argument_type);
}

AggregateFunctionPtr createAggregateFunctionTopK(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & params)
{
    assertUnary(name, argument_types);

    UInt64 threshold = 10; /// default value

    if (!params.empty())
    {
        if (params.size() != 1)
            throw Exception(
                fmt::format("Aggregate function {} requires one parameter or less.", name),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (k > TOP_K_MAX_SIZE)
            throw Exception(
                fmt::format("Too large parameter for aggregate function {}. Maximum: {}", name, TOP_K_MAX_SIZE),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (k == 0)
            throw Exception(
                fmt::format("Parameter 0 is illegal for aggregate function {}", name),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        threshold = k;
    }

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionTopK>(*argument_types[0], threshold));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes(argument_types[0], threshold));

    if (!res)
        throw Exception(
            fmt::format("Illegal type {} of argument for aggregate function {}", argument_types[0]->getName(), name),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

} // namespace

void registerAggregateFunctionTopK(AggregateFunctionFactory & factory)
{
    factory.registerFunction("topK", createAggregateFunctionTopK);
}

} // namespace DB

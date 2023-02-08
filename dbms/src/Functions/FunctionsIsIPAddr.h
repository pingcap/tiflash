// Copyright 2023 PingCAP, Ltd.
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

#pragma once

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <arpa/inet.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

/** Helper functions
  *
  * isIPv4(x) - Judge whether the input string is an IPv4 address.
  *
  * isIPv6(x) - Judge whether the input string is an IPv6 address
  *
  */

static inline UInt8 isIPv4(String input_address)
{
    char str[INET_ADDRSTRLEN];
    if (input_address.empty())
        return 0;
    if (inet_pton(AF_INET, input_address.c_str(), str) == 1)
        return 1;
    return 0;
}

static inline UInt8 isIPv6(String input_address)
{
    char str[INET6_ADDRSTRLEN];
    if (input_address.empty())
        return 0;
    if (inet_pton(AF_INET6, input_address.c_str(), str) == 1)
        return 1;
    return 0;
}

class FunctionIsIPv4 : public IFunction
{
public:
    static constexpr auto name = "tiDBIsIPv4";
    FunctionIsIPv4() = default;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIsIPv4>(); };

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 1.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeUInt8>();
    }
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IColumn * c0_col = block.getByPosition(arguments[0]).column.get();

        Field res_field;
        int val_num = c0_col->size();
        auto col_res = ColumnUInt8::create();
        col_res->reserve(val_num);

        for (int i = 0; i < val_num; i++)
        {
            c0_col->get(i, res_field);
            String handled_str = res_field.get<String>();
            col_res->insert(static_cast<UInt8>(isIPv4(handled_str)));
        }

        block.getByPosition(result).column = std::move(col_res);
    }

private:
};

class FunctionIsIPv6 : public IFunction
{
public:
    static constexpr auto name = "tiDBIsIPv6";
    FunctionIsIPv6() = default;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIsIPv6>(); };

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 1.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeUInt8>();
    }
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IColumn * c0_col = block.getByPosition(arguments[0]).column.get();

        Field res_field;
        int val_num = c0_col->size();
        auto col_res = ColumnUInt8::create();
        col_res->reserve(val_num);

        for (int i = 0; i < val_num; i++)
        {
            c0_col->get(i, res_field);
            String handled_str = res_field.get<String>();
            col_res->insert(static_cast<UInt8>(isIPv6(handled_str)));
        }

        block.getByPosition(result).column = std::move(col_res);
    }

private:
};
} // namespace DB

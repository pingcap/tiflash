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

#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>
#include <Core/AccurateComparison.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/Context.h>
#include <common/types.h>
#include <fmt/core.h>

#include <cstddef>
#include <type_traits>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct LeastImpl
{
    static constexpr auto name = "tidbLeast";
    static bool apply(bool cmp_result) { return cmp_result; }
};
struct GreatestImpl
{
    static constexpr auto name = "tidbGreatest";
    static bool apply(bool cmp_result) { return !cmp_result; }
};

template <typename Impl, typename SpecializedFunction>
class FunctionVectorizedLeastGreatest : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    explicit FunctionVectorizedLeastGreatest(const Context & context)
        : context(context){};
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionVectorizedLeastGreatest<Impl, SpecializedFunction>>(context);
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypePtr type_res = arguments[0];
        auto function = SpecializedFunction{context};
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            DataTypes args{type_res, arguments[i]};
            auto res = function.getReturnTypeImpl(args);
            type_res = std::move(res);
        }
        return type_res;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        size_t num_arguments = arguments.size();
        if (num_arguments < 2)
        {
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        DataTypes data_types(num_arguments);
        for (size_t i = 0; i < num_arguments; ++i)
            data_types[i] = block.getByPosition(arguments[i]).type;

        ColumnWithTypeAndName pre_col = block.getByPosition(arguments[0]);
        auto function_builder = DefaultFunctionBuilder(std::make_shared<SpecializedFunction>(context));
        auto function = SpecializedFunction::create(context);
        ColumnNumbers col_nums = {0, 1};
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            Block temp_block{pre_col, block.getByPosition(arguments[i])};
            DataTypePtr res_type = function_builder.getReturnTypeImpl({pre_col.type, block.getByPosition(arguments[i]).type});
            temp_block.insert({nullptr, res_type, "res_col"});
            function->executeImpl(temp_block, col_nums, 2);
            pre_col = std::move(temp_block.getByPosition(2));
        }
        block.getByPosition(result).column = std::move(pre_col.column);
    }

private:
    const Context & context;
};
template <bool use_collator = true>
struct LeastStringImpl
{
    static void processImpl(
        const TiDB::TiDBCollatorPtr & collator,
        size_t a_size,
        size_t b_size,
        const unsigned char * a_data,
        const unsigned char * b_data,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets)
    {
        int res = 0;
        StringRef s_ra(&a_data[0], a_size);
        StringRef s_rb(&b_data[0], b_size);
        if constexpr (!use_collator)
        {
            res = memcmp(&a_data[0], &b_data[0], std::min(a_size, b_size));
        }
        else
        {
            res = collator->compare(reinterpret_cast<const char *>(&a_data[0]), a_size, reinterpret_cast<const char *>(&b_data[0]), b_size);
        }
        if (res < 0)
        {
            memcpy(&c_data[c_offsets.back()], &a_data[0], a_size);
            c_offsets.push_back(c_offsets.back() + a_size + 1);
        }
        else if (res == 0)
        {
            size_t size = std::min(a_size, b_size);
            memcpy(&c_data[c_offsets.back()], &b_data[0], size);
            c_offsets.push_back(c_offsets.back() + size + 1);
        }
        else
        {
            memcpy(&c_data[c_offsets.back()], &b_data[0], b_size);
            c_offsets.push_back(c_offsets.back() + b_size + 1);
        }
    }
    // string_string
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets,
        size_t i)
    {
        size_t a_size = a_offsets[i] - a_offsets[i - 1] - 1;
        size_t b_size = b_offsets[i] - b_offsets[i - 1] - 1;
        processImpl(collator, a_size, b_size, &a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]], c_data, c_offsets);
    }

    // string_constant
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const String & b,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets,
        size_t i)
    {
        const auto * b_data = reinterpret_cast<const unsigned char *>(b.data());
        ColumnString::Offset b_size = b.size();
        size_t a_size = a_offsets[i] - a_offsets[i - 1] - 1;
        processImpl(collator, a_size, b_size, &a_data[a_offsets[i - 1]], &b_data[0], c_data, c_offsets);
    }

    // constant_constant
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        const std::string & a,
        const std::string & b,
        std::string & c)
    {
        int res = 0;
        if constexpr (!use_collator)
        {
            res = memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
        }
        else
        {
            res = collator->compare(reinterpret_cast<const char *>(a.data()), a.size(), reinterpret_cast<const char *>(b.data()), b.size());
        }
        if (res < 0)
            c = a;
        else if (res == 0)
        {
            size_t size = std::min(a.size(), b.size());
            c = b.substr(0, size);
        }
        else
            c = b;
    }
};

template <bool use_collator>
struct StringOperationImpl
{
    static void NO_INLINE stringVectorStringVector(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        const TiDB::TiDBCollatorPtr & collator,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets)
    {
        size_t size = a_offsets.size();
        c_data.reserve(std::max(a_data.size(), b_data.size()));
        c_data.resize(std::max(a_data.size(), b_data.size()));
        if (size == 0)
            return;
        LeastStringImpl<use_collator>::processImpl(collator, a_offsets[0] - 1, b_offsets[0] - 1, &a_data[0], &b_data[0], c_data, c_offsets);
        for (size_t i = 1; i < size; ++i)
            LeastStringImpl<use_collator>::process(collator, a_data, a_offsets, b_data, b_offsets, c_data, c_offsets, i);
    }

    static void NO_INLINE stringVectorConstant(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const std::string & b,
        const TiDB::TiDBCollatorPtr & collator,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets)
    {
        size_t size = a_offsets.size();
        c_data.reserve(std::max(a_data.size(), b.size() * size));
        c_data.resize(std::max(a_data.size(), b.size() * size));

        const auto * b_data = reinterpret_cast<const unsigned char *>(b.data());
        if (size == 0)
            return;

        LeastStringImpl<use_collator>::processImpl(collator, a_offsets[0] - 1, b.size(), &a_data[0], &b_data[0], c_data, c_offsets);
        for (size_t i = 1; i < size; ++i)
            LeastStringImpl<use_collator>::process(collator, a_data, a_offsets, b, c_data, c_offsets, i);
    }

    static void constantStringVector(
        const std::string & a,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        const TiDB::TiDBCollatorPtr & collator,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets)
    {
        StringOperationImpl::stringVectorConstant(b_data, b_offsets, a, collator, c_data, c_offsets);
    }

    static void constantConstant(
        const std::string & a,
        const std::string & b,
        const TiDB::TiDBCollatorPtr & collator,
        std::string & c)
    {
        LeastStringImpl<use_collator>::process(collator, a, b, c);
    }
};

class FunctionLeastString : public IFunction
{
public:
    static constexpr auto name = "tidbLeastString";
    explicit FunctionLeastString() = default;
    ;
    static FunctionPtr create(const Context & context [[maybe_unused]])
    {
        return std::make_shared<FunctionLeastString>();
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }

    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override
    {
        collator = collator_;
    }

    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto & argument : arguments)
        {
            if (!argument->isString())
            {
                throw Exception(
                    fmt::format("argument type not string"),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        size_t num_arguments = arguments.size();
        if (num_arguments < 2)
        {
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        if (collator)
        {
            executeInternal<true>(block, arguments, result);
        }
        else
        {
            executeInternal<false>(block, arguments, result);
        }
    }
    template <bool use_collator>
    void executeInternal(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        size_t num_arguments = arguments.size();

        using impl = StringOperationImpl<use_collator>;
        const auto * c0 = block.getByPosition(arguments[0]).column.get();
        auto c_res = ColumnString::create();

        for (size_t i = 1; i < num_arguments; ++i)
        {
            if (i > 1)
            {
                c_res = ColumnString::create();
                c0 = block.getByPosition(result).column.get();
            }
            const auto * const c1 = block.getByPosition(arguments[i]).column.get();
            const auto * c0_string = checkAndGetColumn<ColumnString>(c0);
            const auto * c1_string = checkAndGetColumn<ColumnString>(c1);
            const ColumnConst * c0_const = checkAndGetColumnConstStringOrFixedString(c0);
            const ColumnConst * c1_const = checkAndGetColumnConstStringOrFixedString(c1);
            if (c0_const && c1_const)
            {
                String res;
                impl::constantConstant(c0_const->getValue<String>(), c1_const->getValue<String>(), collator, res);
                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(c0_const->size(), toField(res));
            }
            else
            {
                if (c0_string && c1_string)
                    impl::stringVectorStringVector(
                        c0_string->getChars(),
                        c0_string->getOffsets(),
                        c1_string->getChars(),
                        c1_string->getOffsets(),
                        collator,
                        c_res->getChars(),
                        c_res->getOffsets());
                else if (c0_string && c1_const)
                    impl::stringVectorConstant(
                        c0_string->getChars(),
                        c0_string->getOffsets(),
                        c1_const->getValue<String>(),
                        collator,
                        c_res->getChars(),
                        c_res->getOffsets());
                else if (c0_const && c1_string)
                    impl::constantStringVector(
                        c0_const->getValue<String>(),
                        c1_string->getChars(),
                        c1_string->getOffsets(),
                        collator,
                        c_res->getChars(),
                        c_res->getOffsets());

                block.getByPosition(result).column = std::move(c_res);
            }
        }
    }

private:
    TiDB::TiDBCollatorPtr collator{};
};

} // namespace DB

// Copyright 2022 PingCAP, Ltd.
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
#include <Functions/StringUtil.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/Context.h>
#include <common/types.h>
#include <fmt/core.h>

#include <type_traits>

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
template <bool least, bool use_collator = true>
struct LeastGreatestStringImpl
{
    static void processImpl(
        const TiDB::TiDBCollatorPtr & collator,
        size_t a_size,
        size_t b_size,
        const unsigned char * a_data,
        const unsigned char * b_data,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets,
        int i)
    {
        int res = 0;
        StringRef s_ra(&a_data[0], a_size);
        StringRef s_rb(&b_data[0], b_size);
        auto pre_offset = StringUtil::offsetAt(c_offsets, i);
        if constexpr (use_collator)
            res = collator->compare(reinterpret_cast<const char *>(&a_data[0]), a_size, reinterpret_cast<const char *>(&b_data[0]), b_size);

        else
            res = memcmp(&a_data[0], &b_data[0], std::min(a_size, b_size));

        if constexpr (least)
        {
            if (res < 0)
            {
                memcpy(&c_data[pre_offset], &a_data[0], a_size);
                c_offsets[i] = pre_offset + a_size + 1;
            }
            else if (res == 0)
            {
                size_t size = std::min(a_size, b_size);
                memcpy(&c_data[pre_offset], &b_data[0], size);
                c_offsets[i] = pre_offset + size + 1;
            }
            else
            {
                memcpy(&c_data[pre_offset], &b_data[0], b_size);
                c_offsets[i] = pre_offset + b_size + 1;
            }
        }
        else
        {
            if (res < 0)
            {
                memcpy(&c_data[pre_offset], &b_data[0], b_size);
                c_offsets[i] = pre_offset + b_size + 1;
            }
            else if (res == 0)
            {
                size_t size = std::max(a_size, b_size);
                if (a_size > b_size)
                    memcpy(&c_data[pre_offset], &a_data[0], size);
                else
                    memcpy(&c_data[pre_offset], &b_data[0], size);

                c_offsets[i] = pre_offset + size + 1;
            }
            else
            {
                memcpy(&c_data[pre_offset], &a_data[0], a_size);
                c_offsets[i] = pre_offset + a_size + 1;
            }
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
        processImpl(collator, a_size, b_size, &a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]], c_data, c_offsets, i);
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
        processImpl(collator, a_size, b_size, &a_data[a_offsets[i - 1]], &b_data[0], c_data, c_offsets, i);
    }

    // constant_constant
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        const std::string & a,
        const std::string & b,
        std::string & c)
    {
        int res = 0;
        if constexpr (use_collator)
            res = collator->compare(reinterpret_cast<const char *>(a.data()), a.size(), reinterpret_cast<const char *>(b.data()), b.size());
        else
            res = memcmp(a.data(), b.data(), std::min(a.size(), b.size()));

        if constexpr (least)
        {
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
        else
        {
            if (res < 0)
                c = b;
            else if (res == 0)
            {
                if (a.size() > b.size())
                    c = a;
                else
                    c = b;
            }
            else
                c = a;
        }
    }
};

template <bool least, bool use_collator>
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
        c_data.resize(std::max(a_data.size(), b_data.size()));
        if (size == 0)
            return;
        LeastGreatestStringImpl<least, use_collator>::processImpl(collator, a_offsets[0] - 1, b_offsets[0] - 1, &a_data[0], &b_data[0], c_data, c_offsets, 0);
        for (size_t i = 1; i < size; ++i)
            LeastGreatestStringImpl<least, use_collator>::process(collator, a_data, a_offsets, b_data, b_offsets, c_data, c_offsets, i);
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
        c_data.resize(std::max(a_data.size(), b.size() * size));
        const auto * b_data = reinterpret_cast<const unsigned char *>(b.data());
        if (size == 0)
            return;
        LeastGreatestStringImpl<least, use_collator>::processImpl(collator, a_offsets[0] - 1, b.size(), &a_data[0], &b_data[0], c_data, c_offsets, 0);
        for (size_t i = 1; i < size; ++i)
            LeastGreatestStringImpl<least, use_collator>::process(collator, a_data, a_offsets, b, c_data, c_offsets, i);
    }

    static void constantConstant(
        const std::string & a,
        const std::string & b,
        const TiDB::TiDBCollatorPtr & collator,
        std::string & c)
    {
        LeastGreatestStringImpl<least, use_collator>::process(collator, a, b, c);
    }
};

template <bool least>
class FunctionLeastGreatestString : public IFunction
{
public:
    static constexpr auto name = least ? "tidbLeastString" : "tidbGreatestString";
    explicit FunctionLeastGreatestString() = default;

    static FunctionPtr create(const Context & context [[maybe_unused]])
    {
        return std::make_shared<FunctionLeastGreatestString>();
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }

    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }

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
            executeInternal<true>(block, arguments, result);
        else
            executeInternal<false>(block, arguments, result);
    }
    template <bool use_collator>
    void executeInternal(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        size_t num_arguments = arguments.size();

        using impl = StringOperationImpl<least, use_collator>;

        std::vector<const ColumnConst *> const_columns;
        std::vector<const ColumnString *> string_columns;
        for (size_t i = 0; i < num_arguments; ++i)
        {
            const auto * c = block.getByPosition(arguments[i]).column.get();
            const auto * c_string = checkAndGetColumn<ColumnString>(c);
            const ColumnConst * c_const = checkAndGetColumnConstStringOrFixedString(c);
            if (c_const)
                const_columns.emplace_back(c_const);
            if (c_string)
                string_columns.emplace_back(c_string);
        }

        String const_res;
        // for const columns
        if (!const_columns.empty())
        {
            if (const_columns.size() == 1)
            {
                const_res = const_columns[0]->getValue<String>();
            }
            else
            {
                auto res = const_columns[0]->getValue<String>();
                for (size_t i = 1; i < const_columns.size(); ++i)
                    impl::constantConstant(const_columns[i]->getValue<String>(), res, collator, res);
                if (string_columns.empty()) // fill result column
                {
                    block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(const_columns[0]->size(), Field(res));
                    return;
                }
                else
                {
                    const_res = res;
                }
            }
        }

        // for string columns
        std::vector<ColumnString::MutablePtr> results;
        results.emplace_back(ColumnString::create());
        results.emplace_back(ColumnString::create());

        if (string_columns.size() == 1)
            results[0] = ColumnString::create(*string_columns[0]);
        else if (string_columns.size() >= 2)
        {
            for (size_t i = 1; i < string_columns.size(); ++i)
            {
                const DB::ColumnString * c0_string;
                if (i == 1)
                {
                    c0_string = checkAndGetColumn<ColumnString>(string_columns[0]);
                    auto size = c0_string->getOffsets().size();
                    for (auto & res : results)
                        res->getOffsets().resize(size);
                }
                else if (i % 2 == 0)
                    c0_string = checkAndGetColumn<ColumnString>(results[1].get());
                else
                    c0_string = checkAndGetColumn<ColumnString>(results[0].get());
                const auto * c1_string = string_columns[i];
                impl::stringVectorStringVector(
                    c0_string->getChars(),
                    c0_string->getOffsets(),
                    c1_string->getChars(),
                    c1_string->getOffsets(),
                    collator,
                    results[i % 2]->getChars(),
                    results[i % 2]->getOffsets());
            }
            if (const_columns.empty()) // no const columns, use string columns result
            {
                block.getByPosition(result).column = std::move(results[string_columns.size() % 2 == 0]);
                return;
            }
        }
        else // no string columns, use const columns result
        {
            auto col_str = ColumnString::create();
            col_str->insert(Field(const_res));
            auto const_res_col = ColumnConst::create(col_str->getPtr(), const_columns[0]->size());
            block.getByPosition(result).column = std::move(const_res_col);
            return;
        }

        // merge result column of const columns and vector columns
        if (!string_columns.empty() && !const_columns.empty())
        {
            auto & from = results[string_columns.size() % 2 == 0];
            auto & to = results[string_columns.size() % 2];
            auto size = from->getOffsets().size();
            to->getOffsets().resize(size);
            impl::stringVectorConstant(
                from->getChars(),
                from->getOffsets(),
                const_res,
                collator,
                to->getChars(),
                to->getOffsets());

            block.getByPosition(result).column = std::move(to);
            return;
        }
    }

private:
    TiDB::TiDBCollatorPtr collator{};
};

} // namespace DB

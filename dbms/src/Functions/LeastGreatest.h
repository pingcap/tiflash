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
#include <Functions/StringUtil.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/Context_fwd.h>
#include <common/types.h>
#include <fmt/core.h>

#include <memory>
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
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be at least 2.",
                    getName(),
                    arguments.size()),
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
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be at least 2.",
                    getName(),
                    arguments.size()),
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
            DataTypePtr res_type
                = function_builder.getReturnTypeImpl({pre_col.type, block.getByPosition(arguments[i]).type});
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
    static void mergeImpl(
        const TiDB::TiDBCollatorPtr & collator,
        size_t a_size,
        size_t b_size,
        const unsigned char * a_data,
        const unsigned char * b_data,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets,
        size_t i)
    {
        int res = 0;
        if constexpr (use_collator)
            res = collator->compare(
                reinterpret_cast<const char *>(&a_data[0]),
                a_size,
                reinterpret_cast<const char *>(&b_data[0]),
                b_size);
        else
            res = mem_utils::CompareStrView(
                {reinterpret_cast<const char *>(&a_data[0]), a_size},
                {reinterpret_cast<const char *>(&b_data[0]), b_size});

        auto append_data = [&](const unsigned char * data, size_t size) {
            auto pre_offset = StringUtil::offsetAt(c_offsets, i);
            auto expected_reserved_size = pre_offset + size + 1;
            c_data.resize(expected_reserved_size);
            memcpy(&c_data[pre_offset], data, size);
            c_data[pre_offset + size] = 0;
            c_offsets[i] = pre_offset + size + 1;
        };

        if constexpr (least)
        {
            if (res < 0)
            {
                append_data(a_data, a_size);
            }
            else
            {
                assert(res > 0 || a_size == b_size);
                append_data(b_data, b_size);
            }
        }
        else
        {
            if (res < 0)
            {
                append_data(b_data, b_size);
            }
            else
            {
                assert(res > 0 || a_size == b_size);
                append_data(a_data, a_size);
            }
        }
    }

    static void processImpl(
        const TiDB::TiDBCollatorPtr & collator,
        size_t a_size,
        size_t b_size,
        const unsigned char * a_data,
        const unsigned char * b_data,
        std::vector<StringRef> & res_ref,
        size_t i)
    {
        int res = 0;
        if constexpr (use_collator)
            res = collator->compare(
                reinterpret_cast<const char *>(&a_data[0]),
                a_size,
                reinterpret_cast<const char *>(&b_data[0]),
                b_size);
        else
            res = mem_utils::CompareStrView(
                {reinterpret_cast<const char *>(&a_data[0]), a_size},
                {reinterpret_cast<const char *>(&b_data[0]), b_size});

        if constexpr (least)
        {
            if (res < 0)
            {
                res_ref[i] = StringRef(&a_data[0], a_size);
            }
            else if (res == 0)
            {
                size_t size = std::min(a_size, b_size);
                res_ref[i] = StringRef(&b_data[0], size);
            }
            else
            {
                res_ref[i] = StringRef(&b_data[0], b_size);
            }
        }
        else
        {
            if (res < 0)
            {
                res_ref[i] = StringRef(&b_data[0], b_size);
            }
            else if (res == 0)
            {
                if (a_size > b_size)
                    res_ref[i] = StringRef(&a_data[0], a_size);
                else
                    res_ref[i] = StringRef(&b_data[0], b_size);
            }
            else
            {
                res_ref[i] = StringRef(&a_data[0], a_size);
            }
        }
    }

    // StringRef_string
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        std::vector<StringRef> & res_ref,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        size_t i)
    {
        size_t a_size = res_ref[i].size;
        size_t b_size = StringUtil::sizeAt(b_offsets, i) - 1;
        const auto * a_data = reinterpret_cast<const unsigned char *>(res_ref[i].data);
        processImpl(collator, a_size, b_size, a_data, &b_data[b_offsets[i - 1]], res_ref, i);
    }

    // StringRef_constant
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        std::vector<StringRef> & res_ref,
        StringRef & b,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets,
        size_t i)
    {
        size_t a_size = res_ref[i].size;
        ColumnString::Offset b_size = b.size;
        const auto * a_data = reinterpret_cast<const unsigned char *>(res_ref[i].data);
        const auto * b_data = reinterpret_cast<const unsigned char *>(b.data);
        mergeImpl(collator, a_size, b_size, &a_data[0], &b_data[0], c_data, c_offsets, i);
    }

    // string_string
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        std::vector<StringRef> & res_ref,
        size_t i)
    {
        size_t a_size = StringUtil::sizeAt(a_offsets, i) - 1;
        size_t b_size = StringUtil::sizeAt(b_offsets, i) - 1;
        processImpl(collator, a_size, b_size, &a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]], res_ref, i);
    }

    // string_constant
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const StringRef & b,
        std::vector<StringRef> & res_ref,
        size_t i)
    {
        const auto * b_data = reinterpret_cast<const unsigned char *>(b.data);
        ColumnString::Offset b_size = b.size;
        size_t a_size = StringUtil::sizeAt(a_offsets, i) - 1;
        processImpl(collator, a_size, b_size, &a_data[a_offsets[i - 1]], &b_data[0], res_ref, i);
    }

    // string_constant
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const StringRef & b,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets,
        size_t i)
    {
        const auto * b_data = reinterpret_cast<const unsigned char *>(b.data);
        ColumnString::Offset b_size = b.size;
        size_t a_size = StringUtil::sizeAt(a_offsets, i) - 1;
        mergeImpl(collator, a_size, b_size, &a_data[a_offsets[i - 1]], &b_data[0], c_data, c_offsets, i);
    }

    // constant_constant
    static void process(const TiDB::TiDBCollatorPtr & collator, StringRef & a, const StringRef & b)
    {
        int res = 0;
        if constexpr (use_collator)
            res = collator->compare(
                reinterpret_cast<const char *>(a.data),
                a.size,
                reinterpret_cast<const char *>(b.data),
                b.size);
        else
            res = a.compare(b);

        if constexpr (least)
        {
            if (res > 0 || (res == 0 && a.size > b.size))
                a = b;
        }
        else
        {
            if (res < 0 || (res == 0 && a.size < b.size))
                a = b;
        }
    }
};

template <bool least, bool use_collator>
struct StringOperationImpl
{
    static void NO_INLINE stringVectorStringVector(
        const TiDB::TiDBCollatorPtr & collator,
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        std::vector<StringRef> & res_ref)
    {
        size_t size = a_offsets.size();
        for (size_t i = 0; i < size; ++i)
            LeastGreatestStringImpl<least, use_collator>::process(
                collator,
                a_data,
                a_offsets,
                b_data,
                b_offsets,
                res_ref,
                i);
    }

    static void NO_INLINE stringRefVectorStringVector(
        const TiDB::TiDBCollatorPtr & collator,
        std::vector<StringRef> & res_ref,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets)
    {
        size_t size = b_offsets.size();
        for (size_t i = 0; i < size; ++i)
            LeastGreatestStringImpl<least, use_collator>::process(collator, res_ref, b_data, b_offsets, i);
    }

    static void NO_INLINE stringRefVectorConstant(
        const TiDB::TiDBCollatorPtr & collator,
        std::vector<StringRef> & res_ref,
        StringRef & b,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets)
    {
        size_t size = res_ref.size();
        size_t res_ref_size = 0;
        for (auto & ref : res_ref)
            res_ref_size += ref.size + 1;
        c_data.reserve(std::max(res_ref_size, (b.size + 1) * size));
        c_offsets.resize(size);
        for (size_t i = 0; i < size; ++i)
            LeastGreatestStringImpl<least, use_collator>::process(collator, res_ref, b, c_data, c_offsets, i);
    }

    static void NO_INLINE stringVectorConstant(
        const TiDB::TiDBCollatorPtr & collator,
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const StringRef & b,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets)
    {
        size_t size = a_offsets.size();
        c_data.reserve(std::max(a_data.size(), (b.size + 1) * size));
        c_offsets.resize(size);
        for (size_t i = 0; i < size; ++i)
            LeastGreatestStringImpl<least, use_collator>::process(collator, a_data, a_offsets, b, c_data, c_offsets, i);
    }

    static void constantConstant(const TiDB::TiDBCollatorPtr & collator, StringRef & a, StringRef & b)
    {
        LeastGreatestStringImpl<least, use_collator>::process(collator, a, b);
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
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be at least 2.",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto & argument : arguments)
        {
            if (!argument->isString())
            {
                throw Exception(fmt::format("argument type not string"), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be at least 2.",
                    getName(),
                    arguments.size()),
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

        // 1. calculate result column for const columns
        StringRef const_res;
        if (!const_columns.empty())
        {
            const_res = const_columns[0]->getDataAt(0);
            for (size_t i = 1; i < const_columns.size(); ++i)
            {
                StringRef b = const_columns[i]->getDataAt(0);
                impl::constantConstant(collator, const_res, b);
            }

            if (string_columns.empty()) // fill the result column
            {
                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(
                    const_columns[0]->size(),
                    Field(const_res.toString()));
                return;
            }
        }

        // 2. calculate result column for string columns
        auto string_columns_size = string_columns.size();
        if (string_columns_size == 1)
        {
            // 3A. merge result columns of const columns and result_col
            ColumnString * result_col = nullptr;
            result_col = const_cast<ColumnString *>(string_columns[0]);
            auto col_str = ColumnString::create();
            impl::stringVectorConstant(
                collator,
                result_col->getChars(),
                result_col->getOffsets(),
                const_res,
                col_str->getChars(),
                col_str->getOffsets());
            block.getByPosition(result).column = std::move(col_str);
            return;
        }
        else if (string_columns_size >= 2)
        {
            std::vector<StringRef> result_string_refs;
            result_string_refs.resize(string_columns[0]->size());
            for (size_t i = 1; i < string_columns_size; ++i)
            {
                const DB::ColumnString * c0_string;
                const auto * c1_string = string_columns[i];
                if (i == 1)
                {
                    c0_string = checkAndGetColumn<ColumnString>(string_columns[0]);

                    impl::stringVectorStringVector(
                        collator,
                        c0_string->getChars(),
                        c0_string->getOffsets(),
                        c1_string->getChars(),
                        c1_string->getOffsets(),
                        result_string_refs);
                }
                else
                {
                    impl::stringRefVectorStringVector(
                        collator,
                        result_string_refs,
                        c1_string->getChars(),
                        c1_string->getOffsets());
                }
            }
            if (const_columns.empty()) // no const columns, use string columns result
            {
                // materialize string columns result
                auto res_column = ColumnString::create();
                for (auto & ref : result_string_refs)
                    res_column->insertData(ref.data, ref.size);
                block.getByPosition(result).column = std::move(res_column);
                return;
            }
            else
            {
                // 3B. merge result columns of const columns and vector columns
                auto col_str = ColumnString::create();
                impl::stringRefVectorConstant(
                    collator,
                    result_string_refs,
                    const_res,
                    col_str->getChars(),
                    col_str->getOffsets());
                block.getByPosition(result).column = std::move(col_str);
                return;
            }
        }
    }

private:
    TiDB::TiDBCollatorPtr collator{};
};

} // namespace DB

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

#include <Columns/ColumnNullable.h>
#include <Common/typeid_cast.h>
#include <Core/ColumnNumbers.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <common/types.h>
#include <tipb/expression.pb.h>
#include <tipb/metadata.pb.h>

#include <magic_enum.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
} // namespace ErrorCodes

[[maybe_unused]] static bool isPowerOf2(uint64_t num)
{
    return (num & (num - 1)) == 0;
}

using ResultType = UInt64;

class FunctionGrouping
    : public IFunctionBase
    , public IExecutableFunction
    , public std::enable_shared_from_this<FunctionGrouping>
{
public:
    static constexpr auto name = "grouping";
    using ArgType = UInt64; // arg type should always be UInt64

    bool useDefaultImplementationForConstants() const override { return true; }

    FunctionGrouping(const DataTypes & argument_types_, const DataTypePtr & return_type_, const tipb::Expr & expr)
        : argument_types(argument_types_)
        , return_type(return_type_)
    {
        tipb::GroupingFunctionMetadata meta;
        if (!meta.ParseFromString(expr.val()))
            throw Exception("Grouping function decodes meta data fail");

        mode = static_cast<tipb::GroupingMode>(meta.mode());
        size_t num_grouping_mark = meta.grouping_marks_size();

        if (num_grouping_mark <= 0)
            throw Exception("number of grouping_ids should be greater than 0");

        if (mode == tipb::GroupingMode::ModeBitAnd || mode == tipb::GroupingMode::ModeNumericCmp)
        {
            for (const auto & one_grouping_mark : meta.grouping_marks())
            {
                assert(one_grouping_mark.grouping_nums_size() == 1);
                if (mode == tipb::GroupingMode::ModeBitAnd)
                    assert(isPowerOf2(one_grouping_mark.grouping_nums()[0]));
                // should store the meta_grouping_id.
                meta_grouping_ids.emplace_back(one_grouping_mark.grouping_nums()[0]);
            }
        }
        else
        {
            for (const auto & one_grouping_mark : meta.grouping_marks())
            {
                // for every dimension, construct a set.
                std::set<UInt64> grouping_ids;
                for (auto id : one_grouping_mark.grouping_nums())
                {
                    grouping_ids.insert(id);
                }
                meta_grouping_marks.emplace_back(grouping_ids);
            }
        }
    }

    String getName() const override { return name; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getReturnType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const Block & /*sample_block*/) const override
    {
        return std::const_pointer_cast<FunctionGrouping>(shared_from_this());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr & col_grouping_ids = block.getByPosition(arguments[0]).column;
        processGroupingIDs(col_grouping_ids, block.getByPosition(result).column, block.rows());
    }

private:
    void processGroupingIDs(const ColumnPtr & col_grouping_ids, ColumnPtr & col_res, size_t row_num) const
    {
        switch (mode)
        {
        case tipb::GroupingMode::ModeBitAnd:
            groupingVec<tipb::GroupingMode::ModeBitAnd>(col_grouping_ids, col_res, row_num);
            break;
        case tipb::GroupingMode::ModeNumericCmp:
            groupingVec<tipb::GroupingMode::ModeNumericCmp>(col_grouping_ids, col_res, row_num);
            break;
        case tipb::GroupingMode::ModeNumericSet:
            groupingVec<tipb::GroupingMode::ModeNumericSet>(col_grouping_ids, col_res, row_num);
            break;
        default:
            throw Exception(fmt::format("Invalid version {} in grouping function", magic_enum::enum_name(mode)));
        };
    }

    template <tipb::GroupingMode mode>
    void groupingVec(const ColumnPtr & col_grouping_ids, ColumnPtr & col_res, size_t row_num) const
    {
        // get arg's data container
        const auto * grouping_col_vec = checkAndGetColumn<ColumnVector<ArgType>>(&(*col_grouping_ids));
        if (grouping_col_vec == nullptr)
            throw Exception("Arg's data type should be UInt64 in grouping function.");

        const typename ColumnVector<ArgType>::Container & grouping_container = grouping_col_vec->getData();

        // get result's data container
        auto col_vec_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_vec_res->getData();
        vec_res.resize_fill_zero(row_num);

        for (size_t i = 0; i < row_num; ++i)
        {
            if constexpr (mode == tipb::GroupingMode::ModeBitAnd)
                vec_res[i] = groupingImplModeAndBit(grouping_container[i]);
            else if constexpr (mode == tipb::GroupingMode::ModeNumericCmp)
                vec_res[i] = groupingImplModeNumericCmp(grouping_container[i]);
            else if constexpr (mode == tipb::GroupingMode::ModeNumericSet)
                vec_res[i] = groupingImplModeNumericSet(grouping_container[i]);
            else
                throw Exception("Invalid mode in grouping function");
        }
        col_res = std::move(col_vec_res);
    }

    ResultType groupingImplModeAndBit(UInt64 grouping_id) const
    {
        UInt64 res = 0;
        for (auto one_grouping_id : meta_grouping_ids)
        {
            res <<= 1;
            if ((grouping_id & one_grouping_id) <= 0)
                // col is not need, meaning be filled with null and grouped. = 1
                res += 1;
        }
        return res;
    }

    ResultType groupingImplModeNumericCmp(UInt64 grouping_id) const
    {
        UInt64 res = 0;
        for (auto one_grouping_id : meta_grouping_ids)
        {
            res <<= 1;
            if (grouping_id <= one_grouping_id)
                // col is not needed, meaning being filled null and grouped. = 1
                res += 1;
        }
        return res;
    }

    ResultType groupingImplModeNumericSet(UInt64 grouping_id) const
    {
        UInt64 res = 0;
        for (auto one_grouping_mark : meta_grouping_marks)
        {
            res <<= 1;
            auto iter = one_grouping_mark.find(grouping_id);
            if (iter == one_grouping_mark.end())
                // In num-set mode, grouping marks stores those needed-col's grouping set (GIDs).
                // When we can't find the grouping id in set, it means this col is not needed, being filled with null and grouped. = 1
                res += 1;
        }
        return res;
    }

private:
    DataTypes argument_types;
    DataTypePtr return_type;

    tipb::GroupingMode mode;
    // one more dimension for multi grouping function args like: grouping(x,y,z...)
    std::vector<UInt64> meta_grouping_ids;

    // In grouping function, the number of rolled up columns usually very small,
    // so it's appropriate to use std::set as it is faster than unordered_set in
    // small amount of elements.
    // one more dimension for multi grouping function args like: grouping(x,y,z...)
    std::vector<std::set<UInt64>> meta_grouping_marks = {};
};

class FunctionBuilderGrouping : public IFunctionBuilder
{
public:
    static constexpr auto name = "grouping";

    explicit FunctionBuilderGrouping(const Context & /*context*/) {}

    static FunctionBuilderPtr create(const Context & context)
    {
        if (!context.getDAGContext())
        {
            throw Exception("DAGContext should not be nullptr.", ErrorCodes::LOGICAL_ERROR);
        }
        return std::make_shared<FunctionBuilderGrouping>(context);
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForNulls() const override { return true; }
    // at frontend, grouping function can receive maximum number of parameters as 64.
    // at backend, grouping function has been rewritten as receive only gid with meta.
    size_t getNumberOfArguments() const override { return 1; }
    void setExpr(const tipb::Expr & expr_) { expr = expr_; }

protected:
    FunctionBasePtr buildImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & return_type,
        const TiDB::TiDBCollatorPtr & /*collator*/) const override
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return std::make_shared<FunctionGrouping>(data_types, return_type, expr);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t arg_num = arguments.size();
        if (arg_num < 1)
            throw Exception("Too few arguments", ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION);
        else if (arg_num > 1)
            throw Exception("Too many arguments", ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

        RUNTIME_CHECK_MSG(
            arguments[0].type->getTypeId() == TypeIndex::UInt64,
            "Parameter type of grouping function should be UInt64");
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

private:
    tipb::Expr expr;
};

} // namespace DB

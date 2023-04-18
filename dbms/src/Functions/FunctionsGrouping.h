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

#include <Columns/ColumnNullable.h>
#include <Common/typeid_cast.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <common/types.h>
#include <tipb/metadata.pb.h>

#include <magic_enum.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
} // namespace ErrorCodes

static bool isPowerOf2(uint64_t num)
{
    uint64_t flag = 1ul << 63ul;
    bool appear = false;
    for (auto i = 0; i < 64; ++i)
    {
        if ((num & flag) != 0)
        {
            if (appear)
                return false;
            appear = true;
        }
    }

    return true;
}

using ResultType = UInt8;

class FunctionGrouping : public IFunctionBase
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
        size_t num = meta.grouping_marks_size();

        if (num <= 0)
            throw Exception("number of grouping_ids should be greater than 0");

        if (mode == tipb::GroupingMode::ModeBitAnd || mode == tipb::GroupingMode::ModeNumericCmp)
        {
            assert(meta.grouping_marks_size() == 1);
            if (mode == tipb::GroupingMode::ModeBitAnd)
                assert(isPowerOf2(meta.grouping_marks()[0]));
            meta_grouping_id = meta.grouping_marks()[0];
        }
        else
        {
            for (size_t i = 0; i < num; ++i)
                meta_grouping_marks.insert(meta.grouping_marks()[i]);
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
        NullPresence null_presence = getNullPresense(block, arguments);
        if (null_presence.has_null_constant)
        {
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        const ColumnPtr & col_grouping_ids = block.getByPosition(arguments[0]).column;
        processGroupingIDs(col_grouping_ids, block.getByPosition(result).column, block.rows());
    }

private:
    // void processConstGroupingIDs(const ColumnConst * col_grouping_ids_const, ColumnPtr & col_res, size_t row_num) const
    // {
    //     UInt64 grouping_id = col_grouping_ids_const->getUInt(0);
    //     auto res = grouping(grouping_id);

    //     col_res = DataTypeNumber<ResultType>().createColumnConst(row_num, Field(static_cast<UInt64>(res)));
    // }

    void processGroupingIDs(const ColumnPtr & col_grouping_ids, ColumnPtr & col_res, size_t row_num) const
    {
        if (col_grouping_ids->isColumnNullable())
            throw Exception("Grouping function shouldn't get nullable column");
        else
            processVectorGroupingIDs(col_grouping_ids, col_res, row_num);
    }

    void processVectorGroupingIDs(const ColumnPtr & col_grouping_ids, ColumnPtr & col_res, size_t row_num) const
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
        vec_res.resize_fill(row_num, static_cast<ResultType>(0));

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

    ResultType grouping(UInt64 grouping_id) const
    {
        switch (mode)
        {
        case tipb::GroupingMode::ModeBitAnd:
            return groupingImplModeAndBit(grouping_id);
        case tipb::GroupingMode::ModeNumericCmp:
            return groupingImplModeNumericCmp(grouping_id);
        case tipb::GroupingMode::ModeNumericSet:
            return groupingImplModeNumericSet(grouping_id);
        default:
            throw Exception(fmt::format("Invalid version {} in grouping function", magic_enum::enum_name(mode)));
        }
    }

    ResultType groupingImplModeAndBit(UInt64 grouping_id) const
    {
        return (grouping_id & meta_grouping_id) != 0;
    }

    ResultType groupingImplModeNumericCmp(UInt64 grouping_id) const
    {
        return grouping_id > meta_grouping_id;
    }

    ResultType groupingImplModeNumericSet(UInt64 grouping_id) const
    {
        auto iter = meta_grouping_marks.find(grouping_id);
        return iter == meta_grouping_marks.end();
    }

private:
    DataTypes argument_types;
    DataTypePtr return_type;

    tipb::GroupingMode mode;
    UInt64 meta_grouping_id = 0;
    std::set<UInt64> meta_grouping_marks = {};
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
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }
    void setExpr(const tipb::Expr & expr_)
    {
        expr = expr_;
    }

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

        const DataTypePtr & arg_data_type = arguments[0].type;
        if (arg_data_type->isNullable())
        {
            const auto * null_type = checkAndGetDataType<DataTypeNullable>(arg_data_type.get());
            assert(null_type != nullptr);

            const auto & nested_type = null_type->getNestedType();
            if (nested_type->isInteger())
                return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<ResultType>>());
        }
        else if (arg_data_type->isInteger())
        {
            return std::make_shared<DataTypeNumber<ResultType>>();
        }

        throw Exception(fmt::format("Illegal type {} of argument of grouping function", arg_data_type->getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    tipb::Expr expr;
};

} // namespace DB

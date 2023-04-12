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
#include <common/types.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
} // namespace ErrorCodes

class FunctionGrouping : public IFunction
{
public:
    using ResultType = UInt8;
    using ArgType = UInt64; // arg type should always be UInt64
    static constexpr auto name = "grouping";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionGrouping>(); }
    String getName() const override { return name; }
    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t arg_num = arguments.size();
        if (arg_num < 1)
            throw Exception("Too few arguments", ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION);
        else if (arg_num > 1)
            throw Exception("Too many arguments", ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

        const DataTypePtr & arg_data_type = arguments[0];
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        NullPresence null_presence = getNullPresense(block, arguments);
        if (null_presence.has_null_constant)
        {
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        const ColumnPtr & col_grouping_ids = block.getByPosition(arguments[0]).column;
        const auto * col_grouping_ids_const = typeid_cast<const ColumnConst *>(&(*(col_grouping_ids)));

        if (col_grouping_ids_const != nullptr)
            processConstGroupingIDs(col_grouping_ids_const, block.getByPosition(result).column, block.rows());
        else
            processNonConstGroupingIDs(col_grouping_ids, block.getByPosition(result).column, block.rows());
    }

    void setMetaData(const tipb::Expr & expr) override
    {
        const auto & meta_data = expr.groupingmeta();
        version = meta_data.version();
        size_t num = meta_data.grouping_ids_size();

        if (num <= 0)
            throw Exception("number of grouping_ids should be greater than 0");

        if (version == 1 || version == 2)
            meta_grouping_id = meta_data.grouping_ids()[0];
        else
        {
            for (size_t i = 0; i < num; ++i)
                meta_grouping_ids.insert(meta_data.grouping_ids()[i]);
        }
    }

private:
    void processConstGroupingIDs(const ColumnConst * col_grouping_ids_const, ColumnPtr & col_res, size_t row_num) const
    {
        UInt64 grouping_id = col_grouping_ids_const->getUInt(0);
        auto res = grouping(grouping_id);

        col_res = DataTypeNumber<ResultType>().createColumnConst(row_num, Field(static_cast<UInt64>(res)));
    }

    void processNonConstGroupingIDs(const ColumnPtr & col_grouping_ids, ColumnPtr & col_res, size_t row_num) const
    {
        if (col_grouping_ids->isColumnNullable())
            throw Exception("Grouping function shouldn't get nullable column");
        else
            processVectorGroupingIDs(col_grouping_ids, col_res, row_num);
    }

    void processVectorGroupingIDs(const ColumnPtr & col_grouping_ids, ColumnPtr & col_res, size_t row_num) const
    {
        switch (version)
        {
        case 1:
            groupingVec<1>(col_grouping_ids, col_res, row_num);
            break;
        case 2:
            groupingVec<2>(col_grouping_ids, col_res, row_num);
            break;
        case 3:
            groupingVec<3>(col_grouping_ids, col_res, row_num);
            break;
        default:
            throw Exception(fmt::format("Invalid version {} in grouping function", version));
        };
    }

    template <int version>
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
            if constexpr (version == 1)
                vec_res[i] = groupingImplV1(grouping_container[i]);
            else if constexpr (version == 2)
                vec_res[i] = groupingImplV2(grouping_container[i]);
            else if constexpr (version == 3)
                vec_res[i] = groupingImplV3(grouping_container[i]);
            else
                throw Exception("Invalid version in grouping function");
        }
        col_res = std::move(col_vec_res);
    }

    ResultType grouping(UInt64 grouping_id) const
    {
        switch (version)
        {
        case 1:
            return groupingImplV1(grouping_id);
        case 2:
            return groupingImplV2(grouping_id);
        case 3:
            return groupingImplV3(grouping_id);
        default:
            throw Exception(fmt::format("Invalid version {} in grouping function", version));
        }
    }

    ResultType groupingImplV1(UInt64 grouping_id) const
    {
        return (grouping_id & meta_grouping_id) != 0;
    }

    ResultType groupingImplV2(UInt64 grouping_id) const
    {
        return grouping_id > meta_grouping_id;
    }

    ResultType groupingImplV3(UInt64 grouping_id) const
    {
        auto iter = meta_grouping_ids.find(grouping_id);
        return iter == meta_grouping_ids.end();
    }

private:
    TiDB::TiDBCollatorPtr collator = nullptr;

    UInt32 version = 0;
    UInt64 meta_grouping_id = 0;
    std::set<UInt64> meta_grouping_ids = {};
};

} // namespace DB

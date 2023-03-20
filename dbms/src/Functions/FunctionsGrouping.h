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

#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/typeid_cast.h>
#include <Core/ColumnNumbers.h>
#include "Columns/ColumnNullable.h"
#include "common/types.h"

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
} // namespace ErrorCodes

struct NameGrouping
{
    static constexpr auto name = "grouping";
};

template <typename Name>
class FunctionGrouping : public IFunction
{
public:
    using ResultType = UInt8;
    using ArgType = UInt64; // arg type should always be UInt64
    static constexpr auto name = Name::name;

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
        else
        {
            throw Exception(fmt::format("Illegal type {} of argument of grouping function", arg_data_type->getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
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

private:
    void processConstGroupingIDs(const ColumnConst * col_grouping_ids_const, ColumnPtr & col_res, size_t row_num) const
    {
        UInt64 grouping_id = col_grouping_ids_const->getUInt(0);
        auto res = grouping(grouping_id);

        col_res = DataTypeNumber<ResultType>().createColumnConst(row_num, Field(res));
    }

    void processNonConstGroupingIDs(const ColumnPtr & col_grouping_ids, ColumnPtr & col_res, size_t row_num) const
    {
        if (col_grouping_ids->isColumnNullable())
            processNullableGroupingIDs(static_cast<const ColumnNullable &>(*col_grouping_ids), col_res, row_num);
        else
            processVectorGroupingIDs(col_grouping_ids, col_res, row_num);
    }

    void processNullableGroupingIDs(const ColumnNullable & col_grouping_ids, ColumnPtr & col_res, size_t row_num) const
    {
        switch (getVersion())
        {
        case 1:
            groupingNullableVec<1>(col_grouping_ids, col_res, row_num);
            break;
        case 2:
            groupingNullableVec<2>(col_grouping_ids, col_res, row_num);
            break;
        case 3:
            groupingNullableVec<3>(col_grouping_ids, col_res, row_num);
            break;
        default:
            throw Exception(fmt::format("Invalid version {} in grouping function", getVersion()));
        };
    }

    void processVectorGroupingIDs(const ColumnPtr & col_grouping_ids, ColumnPtr & col_res, size_t row_num) const
    {
        switch (getVersion())
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
            throw Exception(fmt::format("Invalid version {} in grouping function", getVersion()));
        };
    }

    UInt32 getVersion() const
    {
        // TODO
        return 0;
    }

    UInt64 getMetaGroupingID() const
    {
        // TODO
        return 0;
    }

    std::set<UInt64> getMetaGroupingIDs() const
    {
        // TODO
        return std::set<UInt64>();
    }

    template <int version>
    void groupingNullableVec(const ColumnNullable & col_grouping_ids, ColumnPtr & col_res, size_t row_num) const
    {
        // get arg's data container and null map
        const auto & grouping_nested_col_data = col_grouping_ids.getNestedColumnPtr();
        const auto * grouping_col_vec = checkAndGetColumn<ColumnVector<ArgType>>(&(*grouping_nested_col_data));
        if (grouping_col_vec == nullptr)
            throw Exception(fmt::format("Arg's data type should be UInt64 in grouping function, but get {}.", typeid(&(*grouping_nested_col_data)).name()));
        const typename ColumnVector<ArgType>::Container & grouping_container = grouping_col_vec->getData();
        const auto & grouping_null_map = col_grouping_ids.getNullMapData();

        // get result's data container
        auto col_vec_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_vec_res->getData();
        vec_res.assign(row_num, 0);

        // get result's null map
        auto nullmap_col_res = ColumnUInt8::create();
        typename ColumnUInt8::Container & res_null_map = nullmap_col_res->getData();
        res_null_map.assign(row_num, 1);

        for (size_t i = 0; i < row_num; ++i)
        {
            if (grouping_null_map[i] == 1)
                continue; // null_map has been set to 1 when initialized

            res_null_map[i] = 0;
            if constexpr (version == 1)
                vec_res[i] = groupingImplV1(grouping_container[i], getMetaGroupingID());
            else if constexpr (version == 2)
                vec_res[i] = groupingImplV2(grouping_container[i], getMetaGroupingID());
            else if constexpr (version == 3)
                vec_res[i] = groupingImplV3(grouping_container[i], getMetaGroupingIDs());
        }
        col_res = ColumnNullable::create(std::move(col_vec_res), std::move(nullmap_col_res));
    }

    template <int version>
    void groupingVec(const ColumnPtr & col_grouping_ids, ColumnPtr & col_res, size_t row_num) const
    {
        // get arg's data container
        const auto * grouping_col_vec = checkAndGetColumn<ColumnVector<ArgType>>(&(*col_grouping_ids));
        if (grouping_col_vec == nullptr)
            throw Exception(fmt::format("Arg's data type should be UInt64 in grouping function, but get {}.", typeid(&(*col_grouping_ids)).name()));
        const typename ColumnVector<ArgType>::Container & grouping_container = grouping_col_vec->getData();

        // get result's data container
        auto col_vec_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_vec_res->getData();
        vec_res.assign(row_num, 0);

        for (size_t i = 0; i < row_num; ++i)
        {
            if constexpr (version == 1)
                vec_res[i] = groupingImplV1(grouping_container[i], getMetaGroupingID());
            else if constexpr (version == 2)
                vec_res[i] = groupingImplV2(grouping_container[i], getMetaGroupingID());
            else if constexpr (version == 3)
                vec_res[i] = groupingImplV3(grouping_container[i], getMetaGroupingIDs());
        }
        col_res = std::move(col_vec_res);
    }

    ResultType grouping(UInt64 grouping_id) const
    {
        switch (getVersion())
        {
        case 1:
            return groupingImplV1(grouping_id, getMetaGroupingID());
        case 2:
            return groupingImplV2(grouping_id, getMetaGroupingID());
        case 3:
            return groupingImplV3(grouping_id, getMetaGroupingIDs());
        default:
            throw Exception(fmt::format("Invalid version {} in grouping function", getVersion()));
        }
    }

    ResultType groupingImplV1(UInt64 grouping_id, UInt64 meta_grouping_id) const
    {
        return (grouping_id & meta_grouping_id) != 0;
    }

    ResultType groupingImplV2(UInt64 grouping_id, UInt64 meta_grouping_id) const
    {
        return grouping_id > meta_grouping_id;
    }

    ResultType groupingImplV3(UInt64 grouping_id, const std::set<UInt64> & meta_grouping_ids) const
    {
        auto iter = meta_grouping_ids.find(grouping_id);
        return iter == meta_grouping_ids.end();
    }

    TiDB::TiDBCollatorPtr collator = nullptr;
};

} // namespace DB

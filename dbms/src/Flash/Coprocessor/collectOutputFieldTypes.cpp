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

#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/collectOutputFieldTypes.h>
#include <TiDB/Schema/TiDB.h>
#include <common/types.h>

namespace DB
{
namespace
{
bool collectForSender(std::vector<tipb::FieldType> & output_field_types, const tipb::ExchangeSender & sender)
{
    if (!sender.all_field_types().empty())
    {
        for (const auto & field_type : sender.all_field_types())
            output_field_types.push_back(field_type);
        return false;
    }
    else
    {
        return true;
    }
}

bool collectForProject(std::vector<tipb::FieldType> & output_field_types, const tipb::Projection & project)
{
    for (const auto & expr : project.exprs())
        output_field_types.push_back(expr.field_type());
    return false;
}

bool collectForAgg(std::vector<tipb::FieldType> & output_field_types, const tipb::Aggregation & agg)
{
    for (const auto & expr : agg.agg_func())
    {
        if (unlikely(!exprHasValidFieldType(expr)))
            throw TiFlashException("Agg expression without valid field type", Errors::Coprocessor::BadRequest);
        output_field_types.push_back(expr.field_type());
    }
    for (const auto & expr : agg.group_by())
    {
        if (unlikely(!exprHasValidFieldType(expr)))
            throw TiFlashException("Group by expression without valid field type", Errors::Coprocessor::BadRequest);
        output_field_types.push_back(expr.field_type());
    }
    return false;
}

bool collectForExecutor(std::vector<tipb::FieldType> & output_field_types, const tipb::Executor & executor);
bool collectForWindow(std::vector<tipb::FieldType> & output_field_types, const tipb::Executor & executor)
{
    // collect output_field_types of child
    getChildren(executor).forEach([&output_field_types](const tipb::Executor & child) {
        traverseExecutorTree(child, [&output_field_types](const tipb::Executor & e) {
            return collectForExecutor(output_field_types, e);
        });
    });

    for (const auto & expr : executor.window().func_desc())
    {
        if (unlikely(!exprHasValidFieldType(expr)))
            throw TiFlashException("Window expression without valid field type", Errors::Coprocessor::BadRequest);
        output_field_types.push_back(expr.field_type());
    }
    return false;
}

bool collectForReceiver(std::vector<tipb::FieldType> & output_field_types, const tipb::ExchangeReceiver & receiver)
{
    for (const auto & field_type : receiver.field_types())
        output_field_types.push_back(field_type);
    return false;
}

template <typename TableScanType>
bool collectForTableScan(std::vector<tipb::FieldType> & output_field_types, const TableScanType & tbl_scan)
{
    for (const auto & ci : tbl_scan.columns())
    {
        tipb::FieldType field_type;
        field_type.set_tp(ci.tp());
        field_type.set_flag(ci.flag());
        field_type.set_flen(ci.columnlen());
        field_type.set_decimal(ci.decimal());
        for (const auto & elem : ci.elems())
            field_type.add_elems(elem);
        output_field_types.push_back(field_type);
    }
    return false;
}

bool collectForExpand2(std::vector<tipb::FieldType> & output_field_types, const tipb::Expand2 & expand2)
{
    // just collect from the level one.
    for (const auto & expr : expand2.proj_exprs().Get(0).exprs())
        output_field_types.push_back(expr.field_type());
    return false;
}

bool collectForExpand(std::vector<tipb::FieldType> & out_field_types, const tipb::Executor & executor)
{
    auto & out_child_fields = out_field_types;
    // collect output_field_types of children
    getChildren(executor).forEach([&out_child_fields](const tipb::Executor & child) {
        traverseExecutorTree(child, [&out_child_fields](const tipb::Executor & e) {
            return collectForExecutor(out_child_fields, e);
        });
    });

    // make the columns from grouping sets nullable.
    for (const auto & grouping_set : executor.expand().grouping_sets())
    {
        for (const auto & grouping_exprs : grouping_set.grouping_exprs())
        {
            for (const auto & grouping_col : grouping_exprs.grouping_expr())
            {
                // assert that: grouping_col must be the column ref guaranteed by tidb.
                auto column_index = decodeDAGInt64(grouping_col.val());
                RUNTIME_CHECK_MSG(
                    column_index >= 0 || column_index < static_cast<Int64>(out_child_fields.size()),
                    "Column index out of bound");
                out_child_fields[column_index].set_flag(
                    out_child_fields[column_index].flag() & (~TiDB::ColumnFlagNotNull));
            }
        }
    }

    {
        // for additional groupingID column.
        tipb::FieldType field_type{};
        field_type.set_tp(TiDB::TypeLongLong);
        field_type.set_charset("binary");
        field_type.set_collate(TiDB::ITiDBCollator::BINARY);
        // groupingID column should be Uint64 and NOT NULL.
        field_type.set_flag(TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull);
        field_type.set_flen(-1);
        field_type.set_decimal(-1);
        out_field_types.push_back(field_type);
    }
    return false;
}

bool collectForJoin(std::vector<tipb::FieldType> & output_field_types, const tipb::Executor & executor)
{
    // collect output_field_types of children
    std::vector<std::vector<tipb::FieldType>> children_output_field_types;
    children_output_field_types.resize(2);
    size_t child_index = 0;
    // for join, dag_request.has_root_executor() == true, can use getChildren and traverseExecutorTree directly.
    getChildren(executor).forEach([&children_output_field_types, &child_index](const tipb::Executor & child) {
        auto & child_output_field_types = children_output_field_types[child_index++];
        traverseExecutorTree(child, [&child_output_field_types](const tipb::Executor & e) {
            return collectForExecutor(child_output_field_types, e);
        });
    });
    assert(child_index == 2);

    // collect output_field_types for join self
    for (auto & field_type : children_output_field_types[0])
    {
        if (executor.join().join_type() == tipb::JoinType::TypeRightOuterJoin)
        {
            /// the type of left column for right join is always nullable
            auto updated_field_type = field_type;
            updated_field_type.set_flag(
                static_cast<UInt32>(updated_field_type.flag()) & (~static_cast<UInt32>(TiDB::ColumnFlagNotNull)));
            output_field_types.push_back(updated_field_type);
        }
        else
        {
            output_field_types.push_back(field_type);
        }
    }

    /// Note: for all kinds of semi join, the right table column is ignored
    /// but for (anti) left outer semi join, a 1/0 (uint8) field is pushed back
    /// indicating whether right table has matching row(s), see comment in ASTTableJoin::Kind for details.
    if (executor.join().join_type() == tipb::JoinType::TypeLeftOuterSemiJoin
        || executor.join().join_type() == tipb::JoinType::TypeAntiLeftOuterSemiJoin)
    {
        /// Note: within DAGRequest tidb doesn't have specific field type info for this column
        /// therefore, we just use tinyType and default values to construct a new one as tidb does in `PlanBuilder::buildSemiJoin`
        tipb::FieldType field_type{};
        field_type.set_tp(TiDB::TypeTiny);
        field_type.set_charset("binary");
        field_type.set_collate(TiDB::ITiDBCollator::BINARY);
        field_type.set_flag(0);
        field_type.set_flen(-1);
        field_type.set_decimal(-1);
        output_field_types.push_back(field_type);
    }
    else if (
        executor.join().join_type() != tipb::JoinType::TypeSemiJoin
        && executor.join().join_type() != tipb::JoinType::TypeAntiSemiJoin)
    {
        /// for semi/anti semi join, the right table column is ignored
        for (auto & field_type : children_output_field_types[1])
        {
            if (executor.join().join_type() == tipb::JoinType::TypeLeftOuterJoin)
            {
                /// the type of right column for left join is always nullable
                auto updated_field_type = field_type;
                updated_field_type.set_flag(
                    updated_field_type.flag() & (~static_cast<UInt32>(TiDB::ColumnFlagNotNull)));
                output_field_types.push_back(updated_field_type);
            }
            else
            {
                output_field_types.push_back(field_type);
            }
        }
    }
    return false;
}

bool collectForExecutor(std::vector<tipb::FieldType> & output_field_types, const tipb::Executor & executor)
{
    assert(output_field_types.empty());
    switch (executor.tp())
    {
    case tipb::ExecType::TypeExchangeSender:
        return collectForSender(output_field_types, executor.exchange_sender());
    case tipb::ExecType::TypeProjection:
        return collectForProject(output_field_types, executor.projection());
    case tipb::ExecType::TypeAggregation:
    case tipb::ExecType::TypeStreamAgg:
        return collectForAgg(output_field_types, executor.aggregation());
    case tipb::ExecType::TypeWindow:
        // Window will only be pushed down in mpp mode.
        // In mpp mode, ExchangeSender or Sender will return output_field_types directly.
        // If not in mpp mode or debug mode, window executor type is invalid.
        return collectForWindow(output_field_types, executor);
    case tipb::ExecType::TypeExchangeReceiver:
        return collectForReceiver(output_field_types, executor.exchange_receiver());
    case tipb::ExecType::TypeTableScan:
        return collectForTableScan(output_field_types, executor.tbl_scan());
    case tipb::ExecType::TypePartitionTableScan:
        return collectForTableScan(output_field_types, executor.partition_table_scan());
    case tipb::ExecType::TypeJoin:
        return collectForJoin(output_field_types, executor);
    case tipb::ExecType::TypeExpand:
        return collectForExpand(output_field_types, executor);
    case tipb::ExecType::TypeExpand2:
        return collectForExpand2(output_field_types, executor.expand2());
    default:
        return true;
    }
}
} // namespace
std::vector<tipb::FieldType> collectOutputFieldTypes(const tipb::DAGRequest & dag_request)
{
    std::vector<tipb::FieldType> output_field_types;
    traverseExecutors(&dag_request, [&output_field_types](const tipb::Executor & e) {
        return collectForExecutor(output_field_types, e);
    });
    return output_field_types;
}
} // namespace DB

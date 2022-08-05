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

#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <TestUtils/executorSerializer.h>
#include <tipb/executor.pb.h>
#include <tipb/expression.pb.h>
namespace DB::tests
{
namespace
{
template <typename T>
struct IsExpr
{
    static constexpr bool value = false;
};

template <>
struct IsExpr<::tipb::Expr>
{
    static constexpr bool value = true;
};

template <typename Column>
String getColumnTypeName(const Column column)
{
    String name;
    if constexpr (IsExpr<Column>::value == true)
        name = getFieldTypeName(column.field_type().tp());
    else
        name = getFieldTypeName(column.tp());
    return name;
}

template <typename Columns>
void toString(const Columns & columns, FmtBuffer & buf)
{
    assert(columns.size() > 0);
    int bound = columns.size() - 1;
    for (int i = 0; i < bound; ++i)
    {
        buf.fmtAppend("<{}, {}>, ", i, getColumnTypeName(columns.at(i)));
    }
    buf.fmtAppend("<{}, {}>", bound, getColumnTypeName(columns.at(bound)));
}
} // namespace

String ExecutorSerializer::serialize(const tipb::DAGRequest * dag_request)
{
    assert((dag_request->executors_size() > 0) != dag_request->has_root_executor());
    if (dag_request->has_root_executor())
    {
        serialize(dag_request->root_executor(), 0);
        return buf.toString();
    }
    else
    {
        FmtBuffer buffer;
        String prefix;
        traverseExecutors(dag_request, [this, &prefix](const tipb::Executor & executor) {
            assert(executor.has_executor_id());
            buf.fmtAppend("{}{}\n", prefix, executor.executor_id());
            prefix.append(" ");
            return true;
        });
        return buffer.toString();
    }
}

void serializeTableScan(const String & executor_id, const tipb::TableScan & ts, FmtBuffer & buf)
{
    if (ts.columns_size() == 0)
    {
        // no column selected, must be something wrong
        throw TiFlashException("No column is selected in table scan executor", Errors::Coprocessor::BadRequest);
    }
    buf.fmtAppend("{} | {{", executor_id);
    toString(ts.columns(), buf);
    buf.append("}\n");
}

void serializeExpression(const tipb::Expr & expr, FmtBuffer & buf)
{
    if (isFunctionExpr(expr))
    {
        buf.fmtAppend("{}(", getFunctionName(expr));
        buf.joinStr(
            expr.children().begin(),
            expr.children().end(),
            [&](const auto & ex, FmtBuffer &) {
                serializeExpression(ex, buf);
            },
            ", ");
        buf.append(")");
    }
    else
    {
        buf.fmtAppend("<{}, {}>", decodeDAGInt64(expr.val()), getFieldTypeName(expr.field_type().tp()));
    }
}

void serializeSelection(const String & executor_id, const tipb::Selection & sel, FmtBuffer & buf)
{
    buf.fmtAppend("{} | ", executor_id);
    // currently only support "and" function in selection executor.
    buf.joinStr(
        sel.conditions().begin(),
        sel.conditions().end(),
        [&](const auto & expr, FmtBuffer &) {
            serializeExpression(expr, buf);
        },
        " and ");
    buf.append("}\n");
}

void serializeLimit(const String & executor_id, const tipb::Limit & limit, FmtBuffer & buf)
{
    buf.fmtAppend("{} | {}\n", executor_id, limit.limit());
}

void serializeProjection(const String & executor_id, const tipb::Projection & proj, FmtBuffer & buf)
{
    buf.fmtAppend("{} | {{", executor_id);
    buf.joinStr(
        proj.exprs().begin(),
        proj.exprs().end(),
        [&](const auto & expr, FmtBuffer &) {
            serializeExpression(expr, buf);
        },
        ", ");
    buf.append("}\n");
}

void serializeAggregation(const String & executor_id, const tipb::Aggregation & agg, FmtBuffer & buf)
{
    buf.fmtAppend("{} | group_by: {{", executor_id);
    buf.joinStr(
        agg.group_by().begin(),
        agg.group_by().end(),
        [&](const auto & group_by, FmtBuffer &) {
            serializeExpression(group_by, buf);
        },
        ", ");
    buf.append("}, agg_func: {");
    buf.joinStr(
        agg.agg_func().begin(),
        agg.agg_func().end(),
        [&](const auto & func, FmtBuffer &) {
            serializeExpression(func, buf);
        },
        ", ");
    buf.append("}\n");
}

void serializeTopN(const String & executor_id, const tipb::TopN & top_n, FmtBuffer & buf)
{
    buf.fmtAppend("{} | order_by: {{", executor_id);
    buf.joinStr(
        top_n.order_by().begin(),
        top_n.order_by().end(),
        [&](const auto & order_by, FmtBuffer & fb) {
            fb.append("(");
            serializeExpression(order_by.expr(), buf);
            fb.fmtAppend(", desc: {})", order_by.desc());
        },
        ", ");
    buf.fmtAppend("}}, limit: {}\n", top_n.limit());
}

void serializeJoin(const String & executor_id, const tipb::Join & join, FmtBuffer & buf)
{
    buf.fmtAppend("{} | {}, {}. left_join_keys: {{", executor_id, getJoinTypeName(join.join_type()), getJoinExecTypeName(join.join_exec_type()));
    toString(join.left_join_keys(), buf);
    buf.append("}, right_join_keys: {");
    toString(join.right_join_keys(), buf);
    buf.append("}\n");
}

void serializeExchangeSender(const String & executor_id, const tipb::ExchangeSender & sender, FmtBuffer & buf)
{
    buf.fmtAppend("{} | type:{}, {{", executor_id, getExchangeTypeName(sender.tp()));
    toString(sender.all_field_types(), buf);
    buf.append("}\n");
}

void serializeExchangeReceiver(const String & executor_id, const tipb::ExchangeReceiver & receiver, FmtBuffer & buf)
{
    buf.fmtAppend("{} | type:{}, {{", executor_id, getExchangeTypeName(receiver.tp()));
    toString(receiver.field_types(), buf);
    buf.append("}\n");
}

void serializeWindow(const String & executor_id, const tipb::Window & window [[maybe_unused]], FmtBuffer & buf)
{
    buf.fmtAppend("{} | partition_by: {{", executor_id);
    buf.joinStr(
        window.partition_by().begin(),
        window.partition_by().end(),
        [&](const auto & partition_by, FmtBuffer & fb) {
            fb.append("(");
            serializeExpression(partition_by.expr(), buf);
            fb.fmtAppend(", desc: {})", partition_by.desc());
        },
        ", ");
    buf.append("}}, order_by: {");
    buf.joinStr(
        window.order_by().begin(),
        window.order_by().end(),
        [&](const auto & order_by, FmtBuffer & fb) {
            fb.append("(");
            serializeExpression(order_by.expr(), buf);
            fb.fmtAppend(", desc: {})", order_by.desc());
        },
        ", ");
    buf.append("}, func_desc: {");
    buf.joinStr(
        window.func_desc().begin(),
        window.func_desc().end(),
        [&](const auto & func, FmtBuffer &) {
            serializeExpression(func, buf);
        },
        ", ");
    if (window.has_frame())
    {
        buf.append("}, frame: {");
        if (window.frame().has_start())
        {
            buf.fmtAppend("start<{}, {}, {}>", window.frame().start().type(), window.frame().start().unbounded(), window.frame().start().offset());
        }
        if (window.frame().has_end())
        {
            buf.fmtAppend(", end<{}, {}, {}>", window.frame().end().type(), window.frame().end().unbounded(), window.frame().end().offset());
        }
    }
    buf.append("}\n");
}

void serializeSort(const String & executor_id, const tipb::Sort & sort [[maybe_unused]], FmtBuffer & buf)
{
    buf.fmtAppend("{} | isPartialSort: {}, partition_by: {{", executor_id, sort.ispartialsort());
    buf.joinStr(
        sort.byitems().begin(),
        sort.byitems().end(),
        [&](const auto & by, FmtBuffer & fb) {
            fb.append("(");
            serializeExpression(by.expr(), buf);
            fb.fmtAppend(", desc: {})", by.desc());
        },
        ", ");
    buf.append("}\n");
}

void ExecutorSerializer::serialize(const tipb::Executor & root_executor, size_t level)
{
    auto append_str = [&level, this](const tipb::Executor & executor) {
        assert(executor.has_executor_id());
        addPrefix(level);
        switch (executor.tp())
        {
        case tipb::ExecType::TypeTableScan:
            serializeTableScan(executor.executor_id(), executor.tbl_scan(), buf);
            break;
        case tipb::ExecType::TypePartitionTableScan:
            throw TiFlashException("Partition table scan executor is not supported", Errors::Coprocessor::Unimplemented); // todo support partition table scan executor.
        case tipb::ExecType::TypeJoin:
            serializeJoin(executor.executor_id(), executor.join(), buf);
            break;
        case tipb::ExecType::TypeIndexScan:
            // index scan not supported
            throw TiFlashException("IndexScan executor is not supported", Errors::Coprocessor::Unimplemented);
        case tipb::ExecType::TypeSelection:
            serializeSelection(executor.executor_id(), executor.selection(), buf);
            break;
        case tipb::ExecType::TypeAggregation:
        // stream agg is not supported, treated as normal agg
        case tipb::ExecType::TypeStreamAgg:
            serializeAggregation(executor.executor_id(), executor.aggregation(), buf);
            break;
        case tipb::ExecType::TypeTopN:
            serializeTopN(executor.executor_id(), executor.topn(), buf);
            break;
        case tipb::ExecType::TypeLimit:
            serializeLimit(executor.executor_id(), executor.limit(), buf);
            break;
        case tipb::ExecType::TypeProjection:
            serializeProjection(executor.executor_id(), executor.projection(), buf);
            break;
        case tipb::ExecType::TypeKill:
            throw TiFlashException("Kill executor is not supported", Errors::Coprocessor::Unimplemented);
        case tipb::ExecType::TypeExchangeReceiver:
            serializeExchangeReceiver(executor.executor_id(), executor.exchange_receiver(), buf);
            break;
        case tipb::ExecType::TypeExchangeSender:
            serializeExchangeSender(executor.executor_id(), executor.exchange_sender(), buf);
            break;
        case tipb::ExecType::TypeSort:
            serializeSort(executor.executor_id(), executor.sort(), buf);
            break;
        case tipb::ExecType::TypeWindow:
            serializeWindow(executor.executor_id(), executor.window(), buf);
            break;
        default:
            throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
        }
        ++level;
    };

    traverseExecutorTree(root_executor, [&](const tipb::Executor & executor) {
        append_str(executor);
        if (executor.has_join())
        {
            for (const auto & child : executor.join().children())
                serialize(child, level);
            return false;
        }
        return true;
    });
}

} // namespace DB::tests
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
String ExecutorSerializer::serialize(const tipb::DAGRequest * dag_request)
{
    assert((dag_request->executors_size() > 0) != dag_request->has_root_executor());
    if (dag_request->has_root_executor())
    {
        serialize(dag_request->root_executor(), 0);
        return context.buf.toString();
    }
    else
    {
        FmtBuffer buffer;
        String prefix;
        traverseExecutors(dag_request, [this, &prefix](const tipb::Executor & executor) {
            assert(executor.has_executor_id());
            context.buf.fmtAppend("{}{}\n", prefix, executor.executor_id());
            prefix.append(" ");
            return true;
        });
        return buffer.toString();
    }
}

void serializeTableScan(const String & executor_id, const tipb::TableScan & ts, ExecutorSerializerContext & context)
{
    if (ts.columns_size() == 0)
    {
        // no column selected, must be something wrong
        throw TiFlashException("No column is selected in table scan executor", Errors::Coprocessor::BadRequest);
    }
    context.buf.fmtAppend("{} | {{", executor_id);
    int bound = ts.columns_size() - 1;
    for (int i = 0; i < bound; ++i)
    {
        context.buf.fmtAppend("<{}, {}>, ", i, getFieldTypeName(ts.columns(i).tp()));
    }
    context.buf.fmtAppend("<{}, {}>", bound, getFieldTypeName(ts.columns(bound).tp()));
    context.buf.append("}\n");
}

void serializeExpression(const tipb::Expr & expr, ExecutorSerializerContext & context)
{
    if (isFunctionExpr(expr))
    {
        context.buf.fmtAppend("{}(", getFunctionName(expr));
        context.buf.joinStr(
            expr.children().begin(),
            expr.children().end(),
            [&](const auto & co, FmtBuffer &) {
                serializeExpression(co, context);
            },
            ", ");
        context.buf.append(")");
    }
    else
    {
        context.buf.fmtAppend("<{}, {}>", decodeDAGInt64(expr.val()), getFieldTypeName(expr.field_type().tp()));
    }
}

void serializeSelection(const String & executor_id, const tipb::Selection & sel, ExecutorSerializerContext & context)
{
    context.buf.fmtAppend("{} | ", executor_id);
    // currently only support "and" function in selection executor.
    context.buf.joinStr(
        sel.conditions().begin(),
        sel.conditions().end(),
        [&](const auto & expr, FmtBuffer &) {
            serializeExpression(expr, context);
        },
        " and ");
    context.buf.append("}\n");
}

void serializeLimit(const String & executor_id, const tipb::Limit & limit, ExecutorSerializerContext & context)
{
    context.buf.fmtAppend("{} | {}\n", executor_id, limit.limit());
}

void serializeProjection(const String & executor_id, const tipb::Projection & proj, ExecutorSerializerContext & context)
{
    context.buf.fmtAppend("{} | {{", executor_id);
    context.buf.joinStr(
        proj.exprs().begin(),
        proj.exprs().end(),
        [&](const auto & expr, FmtBuffer &) {
            serializeExpression(expr, context);
        },
        ", ");
    context.buf.append("}\n");
}

void serializeAggregation(const String & executor_id, const tipb::Aggregation & agg, ExecutorSerializerContext & context)
{
    context.buf.fmtAppend("{} | group_by: {{", executor_id);
    context.buf.joinStr(
        agg.group_by().begin(),
        agg.group_by().end(),
        [&](const auto & group_by, FmtBuffer &) {
            serializeExpression(group_by, context);
        },
        ", ");
    context.buf.append("}, agg_func: {");
    context.buf.joinStr(
        agg.agg_func().begin(),
        agg.agg_func().end(),
        [&](const auto & func, FmtBuffer &) {
            serializeExpression(func, context);
        },
        ", ");
    context.buf.append("}\n");
}

void serializeTopN(const String & executor_id, const tipb::TopN & top_n, ExecutorSerializerContext & context)
{
    context.buf.fmtAppend("{} | order_by: {{", executor_id);
    context.buf.joinStr(
        top_n.order_by().begin(),
        top_n.order_by().end(),
        [&](const auto & order_by, FmtBuffer & fb) {
            fb.append("(");
            serializeExpression(order_by.expr(), context);
            fb.fmtAppend(", desc: {})", order_by.desc());
        },
        ", ");
    context.buf.fmtAppend("}}, limit: {}\n", top_n.limit());
}

void serializeJoin(const String & executor_id, const tipb::Join & join, ExecutorSerializerContext & context)
{
    assert(join.left_join_keys_size() > 0);
    assert(join.right_join_keys_size() > 0);
    context.buf.fmtAppend("{} | {}, {}. left_join_keys: {{", executor_id, getJoinTypeName(join.join_type()), getJoinExecTypeName(join.join_exec_type()));
    int bound = join.left_join_keys_size() - 1;
    for (int i = 0; i < bound; ++i)
    {
        context.buf.fmtAppend("<{}, {}>, ", i, getFieldTypeName(join.left_join_keys(i).field_type().tp()));
    }
    context.buf.fmtAppend("<{}, {}>", bound, getFieldTypeName(join.left_join_keys(bound).field_type().tp()));
    context.buf.append("}, right_join_keys: {");
    bound = join.right_join_keys_size() - 1;
    for (int i = 0; i < bound; ++i)
    {
        context.buf.fmtAppend("<{}, {}>, ", i, getFieldTypeName(join.right_join_keys(i).field_type().tp()));
    }
    context.buf.fmtAppend("<{}, {}>", bound, getFieldTypeName(join.right_join_keys(bound).field_type().tp()));

    context.buf.append("}\n");
}

void serializeExchangeSender(const String & executor_id, const tipb::ExchangeSender & sender, ExecutorSerializerContext & context)
{
    assert(sender.all_field_types_size() > 0);
    context.buf.fmtAppend("{} | type:{}, {{", executor_id, getExchangeTypeName(sender.tp()));
    int bound = sender.all_field_types_size() - 1;
    for (int i = 0; i < bound; ++i)
    {
        context.buf.fmtAppend("<{}, {}>, ", i, getFieldTypeName(sender.all_field_types(i).tp()));
    }
    context.buf.fmtAppend("<{}, {}>", bound, getFieldTypeName(sender.all_field_types(bound).tp()));
    context.buf.append("}\n");
}

void serializeExchangeReceiver(const String & executor_id, const tipb::ExchangeReceiver & receiver, ExecutorSerializerContext & context)
{
    assert(receiver.field_types_size() > 0);
    context.buf.fmtAppend("{} | type:{}, {{", executor_id, getExchangeTypeName(receiver.tp()));
    int bound = receiver.field_types_size() - 1;
    for (int i = 0; i < bound; ++i)
    {
        context.buf.fmtAppend("<{}, {}>, ", i, getFieldTypeName(receiver.field_types(i).tp()));
    }
    context.buf.fmtAppend("<{}, {}>", bound, getFieldTypeName(receiver.field_types(bound).tp()));
    context.buf.append("}\n");
}

void ExecutorSerializer::serialize(const tipb::Executor & root_executor, size_t level)
{
    auto append_str = [&level, this](const tipb::Executor & executor) {
        assert(executor.has_executor_id());
        addPrefix(level);
        switch (executor.tp())
        {
        case tipb::ExecType::TypeTableScan:
            serializeTableScan(executor.executor_id(), executor.tbl_scan(), context);
            break;
        case tipb::ExecType::TypePartitionTableScan:
            throw TiFlashException("Partition table scan executor is not supported", Errors::Coprocessor::Unimplemented); // todo support partition table scan executor.
        case tipb::ExecType::TypeJoin:
            serializeJoin(executor.executor_id(), executor.join(), context);
            break;
        case tipb::ExecType::TypeIndexScan:
            // index scan not supported
            throw TiFlashException("IndexScan executor is not supported", Errors::Coprocessor::Unimplemented);
        case tipb::ExecType::TypeSelection:
            serializeSelection(executor.executor_id(), executor.selection(), context);
            break;
        case tipb::ExecType::TypeAggregation:
        // stream agg is not supported, treated as normal agg
        case tipb::ExecType::TypeStreamAgg:
            serializeAggregation(executor.executor_id(), executor.aggregation(), context);
            break;
        case tipb::ExecType::TypeTopN:
            serializeTopN(executor.executor_id(), executor.topn(), context);
            break;
        case tipb::ExecType::TypeLimit:
            serializeLimit(executor.executor_id(), executor.limit(), context);
            break;
        case tipb::ExecType::TypeProjection:
            serializeProjection(executor.executor_id(), executor.projection(), context);
            break;
        case tipb::ExecType::TypeKill:
            throw TiFlashException("Kill executor is not supported", Errors::Coprocessor::Unimplemented);
        case tipb::ExecType::TypeExchangeReceiver:
            serializeExchangeReceiver(executor.executor_id(), executor.exchange_receiver(), context);
            break;
        case tipb::ExecType::TypeExchangeSender:
            serializeExchangeSender(executor.executor_id(), executor.exchange_sender(), context);
            break;
        case tipb::ExecType::TypeSort:
            throw TiFlashException("Sort executor is not supported", Errors::Coprocessor::Unimplemented); // todo support sort executor.
        case tipb::ExecType::TypeWindow:
            throw TiFlashException("Window executor is not supported", Errors::Coprocessor::Unimplemented); // todo support window executor.
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
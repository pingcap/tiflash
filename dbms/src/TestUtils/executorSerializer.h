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

#include <Common/FmtUtils.h>
#include <Core/NamesAndTypes.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>
#include <TestUtils/TiFlashTestException.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
class Context;

namespace tests
{
struct ExecutorSerializerContext
{
    Context & context;
    FmtBuffer & buf;

    explicit ExecutorSerializerContext(Context & context_, FmtBuffer & fmt_buf)
        : context(context_)
        , buf(fmt_buf)
    {}
};

void serializeTableScan(const String & executor_id, const tipb::TableScan & ts, ExecutorSerializerContext & context);
void serializeSelection(const String & executor_id, const tipb::Selection & sel, ExecutorSerializerContext & context);
void serializeLimit(const String & executor_id, const tipb::Limit & limit, ExecutorSerializerContext & context);
void serializeProjection(const String & executor_id, const tipb::Projection & proj, ExecutorSerializerContext & context);
void serializeAggregation(const String & executor_id, const tipb::Aggregation & agg, ExecutorSerializerContext & context);
void serializeTopN(const String & executor_id, const tipb::TopN & top_n, ExecutorSerializerContext & context);
void serializeJoin(const String & executor_id, const tipb::Join & join, ExecutorSerializerContext & context);
void serializeExchangeSender(const String & executor_id, const tipb::ExchangeSender & sender, ExecutorSerializerContext & context);
void serializeExchangeReceiver(const String & executor_id, const tipb::ExchangeReceiver & receiver, ExecutorSerializerContext & context);
class ExecutorSerializer
{
public:
    ExecutorSerializer(Context & context_, FmtBuffer & fmt_buf)
        : context(ExecutorSerializerContext(context_, fmt_buf))
    {
    }

    String serialize(const tipb::DAGRequest * dag_request);

private:
    void serialize(const tipb::Executor & root_executor, size_t level)
    {
        auto append_str = [&level, this](const tipb::Executor & executor) {
            assert(executor.has_executor_id());
            addPrefix(level);
            switch (executor.tp())
            {
            case tipb::ExecType::TypeTableScan:
                serializeTableScan(executor.executor_id(), executor.tbl_scan(), context);
                break;
            case tipb::ExecType::TypeJoin:
                serializeJoin(executor.executor_id(), executor.join(), context);
                break;
            case tipb::ExecType::TypeIndexScan:
                // index scan not supported
                throw TiFlashException("IndexScan is not supported", Errors::Coprocessor::Unimplemented);
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
            default:
                throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
            }
            ++level;
        };

        traverseExecutorTree(root_executor, [&](const tipb::Executor & executor) {
            if (executor.has_join())
            {
                append_str(executor);
                for (const auto & child : executor.join().children())
                    serialize(child, level);
                return false;
            }
            else
            {
                append_str(executor);
                return true;
            }
        });
    }

    void addPrefix(size_t level)
    {
        for (size_t i = 0; i < level; ++i)
            context.buf.append(" ");
    }

private:
    ExecutorSerializerContext context;
};
} // namespace tests

} // namespace DB
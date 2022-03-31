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

#include <Flash/Statistics/traverseExecutors.h>

#include <cstddef>

#include "Common/FmtUtils.h"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Core/NamesAndTypes.h>

namespace DB
{
class Context;

struct SerializeExecutorContext
{
    Context & context;
    FmtBuffer & buf;

    SerializeExecutorContext(Context & context_, FmtBuffer & buf_)
        : context(context_)
        , buf(buf_)
    {}
};

void buildTSString(const String & executor_id, const tipb::TableScan & ts, SerializeExecutorContext & serialize_executor_context);
void buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, SerializeExecutorContext & serialize_executor_context);
void buildSelString(const String & executor_id, const tipb::Selection & sel, SerializeExecutorContext & serialize_executor_context);
void buildLimitString(const String & executor_id, const tipb::Limit & limit, SerializeExecutorContext & serialize_executor_context);
void buildProjString(const String & executor_id, const tipb::Projection & proj, SerializeExecutorContext & serialize_executor_context);
void buildAggString(const String & executor_id, const tipb::Aggregation & agg, SerializeExecutorContext & serialize_executor_context);
void buildTopNString(const String & executor_id, const tipb::TopN & top_n, SerializeExecutorContext & serialize_executor_context);
void buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, SerializeExecutorContext & serialize_executor_context);
void buildJoinString(const String & executor_id, const tipb::Join & join, SerializeExecutorContext & serialize_executor_context);


class SerializeExecutor
{
public:
    SerializeExecutor(Context & context_, FmtBuffer & buf)
        : context(context_, buf)
    {
    }

    String serialize(const tipb::DAGRequest * dag_request)
    {
        assert((dag_request->executors_size() > 0) != dag_request->has_root_executor());
        if (dag_request->has_root_executor())
        {
            toString(dag_request->root_executor(), 0);
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
            // ywq todo check and remove it?
        }
    }

private:
    // ywq todo more detailed infomation.
    void toString(const tipb::Executor & root_executor, size_t level)
    {
        auto append_str = [&level, this](const tipb::Executor & executor) {
            assert(executor.has_executor_id());
            addPrefix(level);

            switch (executor.tp())
            {
            case tipb::ExecType::TypeTableScan:
                buildTSString(executor.executor_id(), executor.tbl_scan(), context);
                break;
            case tipb::ExecType::TypeJoin:
                buildJoinString(executor.executor_id(), executor.join(), context);
                break;
            case tipb::ExecType::TypeIndexScan:
                // index scan not supported
                throw TiFlashException("IndexScan is not supported", Errors::Coprocessor::Unimplemented);
            case tipb::ExecType::TypeSelection:
                buildSelString(executor.executor_id(), executor.selection(), context);
                break;
            case tipb::ExecType::TypeAggregation:
            // stream agg is not supported, treated as normal agg
            case tipb::ExecType::TypeStreamAgg:
                buildAggString(executor.executor_id(), executor.aggregation(), context);
                break;
            case tipb::ExecType::TypeTopN:
                buildTopNString(executor.executor_id(), executor.topn(), context);
                break;
            case tipb::ExecType::TypeLimit:
                buildLimitString(executor.executor_id(), executor.limit(), context);
                break;
            case tipb::ExecType::TypeProjection:
                buildProjString(executor.executor_id(), executor.projection(), context);
                break;
            case tipb::ExecType::TypeExchangeSender:
                buildExchangeSenderString(executor.executor_id(), executor.exchange_sender(), context);
                break;
            case tipb::ExecType::TypeExchangeReceiver:
                buildExchangeReceiverString(executor.executor_id(), executor.exchange_receiver(), context);
                break;
            case tipb::ExecType::TypeKill:
                throw TiFlashException("Kill executor is not supported", Errors::Coprocessor::Unimplemented);
            default:
                throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
            }
        }; // to support more executor types

        traverseExecutorTree(root_executor, [&](const tipb::Executor & executor) {
            if (executor.has_join())
            {
                for (const auto & child : executor.join().children())
                    toString(child, level);
                return false;
            }
            else
            {
                append_str(executor);
                ++level;
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
    SerializeExecutorContext context;
};


} // namespace DB
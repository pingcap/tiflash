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

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsBase.h>
#include <Flash/Statistics/transformProfiles.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

#include <memory>
#include <vector>

namespace DB
{
template <typename ExecutorImpl>
class ExecutorStatistics : public ExecutorStatisticsBase
{
public:
    ExecutorStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
        : dag_context(dag_context_)
    {
        assert(executor->has_executor_id());
        executor_id = executor->executor_id();

        type = ExecutorImpl::type;
        is_source_executor = ExecutorImpl::isSourceExecutor();
    }

    void setChild(const String & child_id) override { children.push_back(child_id); }

    void setChildren(const std::vector<String> & children_) override
    {
        children.insert(children.end(), children_.begin(), children_.end());
    }

    String toJson() const override
    {
        FmtBuffer fmt_buffer;
        fmt_buffer.fmtAppend(R"({{"id":"{}","type":"{}","children":[)", executor_id, type);
        fmt_buffer.joinStr(
            children.cbegin(),
            children.cend(),
            [](const String & child, FmtBuffer & bf) { bf.fmtAppend(R"("{}")", child); },
            ",");
        fmt_buffer.fmtAppend(
            R"(],"outbound_rows":{},"outbound_blocks":{},"outbound_bytes":{},"outbound_allocated_bytes":{},"concurrency":{},"execution_time_ns":{})",
            base.rows,
            base.blocks,
            base.bytes,
            base.allocated_bytes,
            base.concurrency,
            base.execution_time_ns);
        if constexpr (ExecutorImpl::has_extra_info)
        {
            fmt_buffer.append(",");
            appendExtraJson(fmt_buffer);
        }
        fmt_buffer.append("}");
        return fmt_buffer.toString();
    }

    void collectRuntimeDetail() override
    {
        switch (dag_context.getExecutionMode())
        {
        case ExecutionMode::None:
            break;
        case ExecutionMode::Stream:
            transformProfileForStream(dag_context, executor_id, [&](const IProfilingBlockInputStream & p_stream) {
                base.append(p_stream.getProfileInfo());
            });
            // Special handling of join build time is only required for streams.
            collectJoinBuildTime();
            break;
        case ExecutionMode::Pipeline:
            transformProfileForPipeline(dag_context, executor_id, [&](const OperatorProfileInfo & profile_info) {
                base.append(profile_info);
            });
            break;
        }

        if constexpr (ExecutorImpl::has_extra_info)
        {
            collectExtraRuntimeDetail();
        }
    }

    static bool isMatch(const tipb::Executor * executor) { return ExecutorImpl::isMatch(executor); }

protected:
    String executor_id;
    String type;

    std::vector<String> children;

    DAGContext & dag_context;

    virtual void appendExtraJson(FmtBuffer &) const {}

    virtual void collectExtraRuntimeDetail() {}

    void collectJoinBuildTime()
    {
        /// for join need to add the build time
        /// In TiFlash, a hash join's build side is finished before probe side starts,
        /// so the join probe side's running time does not include hash table's build time,
        /// when construct Execution Summaries, we need add the build cost to probe executor
        auto all_join_id_it = dag_context.getExecutorIdToJoinIdMap().find(executor_id);
        if (all_join_id_it != dag_context.getExecutorIdToJoinIdMap().end())
        {
            for (const auto & join_executor_id : all_join_id_it->second)
            {
                auto it = dag_context.getJoinExecuteInfoMap().find(join_executor_id);
                if (it != dag_context.getJoinExecuteInfoMap().end())
                {
                    UInt64 time = 0;
                    for (const auto & join_build_stream : it->second.join_build_streams)
                    {
                        if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(join_build_stream.get());
                            p_stream)
                            time = std::max(time, p_stream->getProfileInfo().execution_time);
                    }
                    process_time_for_join_build += time;
                }
            }
        }
    }
};
} // namespace DB

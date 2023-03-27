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

#pragma once

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsBase.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <common/types.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <tipb/executor.pb.h>

#include <memory>
#include <vector>

namespace DB
{
class DAGContext;

template <typename ExecutorImpl>
class ExecutorStatistics : public ExecutorStatisticsBase
{
public:
    ExecutorStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
        : dag_context(dag_context_)
    {
        RUNTIME_CHECK(executor->has_executor_id());
        executor_id = executor->executor_id();

        type = ExecutorImpl::type;
    }

    void setChild(const String & child_id) override
    {
        children.push_back(child_id);
    }

    void setChildren(const std::vector<String> & children_) override
    {
        children.insert(children.end(), children_.begin(), children_.end());
    }

    String toJson() const override
    {
        FmtBuffer fmt_buffer;
        fmt_buffer.fmtAppend(
            R"({{"id":"{}","type":"{}","children":[)",
            executor_id,
            type);
        fmt_buffer.joinStr(
            children.cbegin(),
            children.cend(),
            [](const String & child, FmtBuffer & bf) { bf.fmtAppend(R"("{}")", child); },
            ",");
        fmt_buffer.fmtAppend(
            R"(],"outbound_rows":{},"outbound_blocks":{},"outbound_bytes":{},"execution_time_ns":{})",
            base.rows,
            base.blocks,
            base.bytes,
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
        const auto & profile_streams_map = dag_context.getProfileStreamsMap();
        auto it = profile_streams_map.find(executor_id);
        if (it != profile_streams_map.end())
        {
            for (const auto & input_stream : it->second)
            {
                if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(input_stream.get()); p_stream)
                {
                    const auto & profile_info = p_stream->getProfileInfo();
                    base.append(profile_info);
                }
            }
        }

        if constexpr (ExecutorImpl::has_extra_info)
        {
            collectExtraRuntimeDetail();
        }

        collectJoinBuildTime();
    }

    void collectRuntimeDetailPipeline() override
    {
        const auto & pipeline_profiles_map = dag_context.getPipelineProfilesMap();
        auto it = pipeline_profiles_map.find(executor_id);

        if (it != pipeline_profiles_map.end())
        {
            // 1. Calculate time_processed_ns for operators before the last operator
            const auto & executor_profile = it->second;
            size_t profile_num = executor_profile.size();
            if (profile_num == 0)
            {
                return;
            }

            for (size_t i = 0; i < profile_num - 1; ++i)
            {
                auto && operator_profile_group = executor_profile[i];
                UInt64 time_processed_ns = 0;
                // time_processed_ns = max(time_processed_ns of all operator in one group)
                for (const auto & operator_profile : operator_profile_group)
                    time_processed_ns = std::max(time_processed_ns, operator_profile->execution_time);
                base.execution_time_ns += time_processed_ns;
            }

            // 2. Only output the last operator's num_produced_rows, num_iterations and concurrency
            auto && operator_profile_group = executor_profile[profile_num - 1];
            UInt64 time_processed_ns = 0;
            base.concurrency = operator_profile_group.size();
            for (const auto & operator_profile : operator_profile_group)
            {
                time_processed_ns = std::max(time_processed_ns, operator_profile->execution_time);
                base.rows += operator_profile->rows;
                base.blocks += operator_profile->blocks;
            }
            base.execution_time_ns += time_processed_ns;
        }
    }

    static bool isMatch(const tipb::Executor * executor)
    {
        return ExecutorImpl::isMatch(executor);
    }

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
                        if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(join_build_stream.get()); p_stream)
                            time = std::max(time, p_stream->getProfileInfo().execution_time);
                    }
                    process_time_for_join_build += time;
                }
            }
        }
    }
};
} // namespace DB

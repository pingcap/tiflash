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

#include <Interpreters/Join.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <mutex>
#include "Common/Exception.h"

namespace DB
{
class JoinSpillContext : public std::enable_shared_from_this<JoinSpillContext>
{
public:
    JoinSpillContext(
        PipelineExecutorContext & exec_context_,
        const JoinPtr & join_)
        : exec_context(exec_context_)
        , join(join_)
    {
        exec_context.incActiveRefCount();
    }

    ~JoinSpillContext()
    {
        join.reset();
        // To ensure that `PipelineExecutorContext` will not be destructed before `JoinSpillContext` is destructed.
        exec_context.decActiveRefCount();
    }

    template<bool is_build_side>
    void init(size_t concurrency_)
    {
        RUNTIME_CHECK(concurrency_ > 0);
        std::lock_guard lock(mu);
        if constexpr (is_build_side)
        {
            RUNTIME_CHECK(build_side_spilling_tasks.empty());
            build_side_spilling_tasks.resize(concurrency_, 0);
        }
        else
        {
            RUNTIME_CHECK(probe_side_spilling_tasks.empty());
            probe_side_spilling_tasks.resize(concurrency_, 0);
        }
    }

    template<bool is_build_side>
    bool isSpilling(size_t op_index = default_op_index)
    {
        std::lock_guard lock(mu);
        if constexpr (is_build_side)
        {
            if (build_side_spilling_tasks.empty())
                return false;
            RUNTIME_CHECK(op_index < build_side_spilling_tasks.size());
            return build_side_spilling_tasks[op_index] > 0;
        }
        else
        {
            if (probe_side_spilling_tasks.empty())
                return false;
            RUNTIME_CHECK(op_index < probe_side_spilling_tasks.size());
            return probe_side_spilling_tasks[op_index] > 0;
        }
    }

    template<bool is_build_side>
    void spillBlocks(UInt64 part_id, Blocks && blocks, size_t op_index = default_op_index)
    {
        startSpilling<is_build_side>(op_index);
        RUNTIME_CHECK(join->build_spiller);
        if constexpr (is_build_side)
        {
            join->build_spiller->spillBlocks(std::move(blocks), part_id);
        }
        else
        {
            join->probe_spiller->spillBlocks(std::move(blocks), part_id);
        }
        finishSpilling<is_build_side>(op_index);
    }

private:
    template<bool is_build_side>
    void startSpilling(size_t op_index = default_op_index)
    {
        std::lock_guard lock(mu);
        if constexpr (is_build_side)
        {
            RUNTIME_CHECK(op_index < build_side_spilling_tasks.size());
            ++build_side_spilling_tasks[op_index];
        }
        else
        {
            RUNTIME_CHECK(op_index < probe_side_spilling_tasks.size());
            ++probe_side_spilling_tasks[op_index];
        }
    }

    template<bool is_build_side>
    void finishSpilling(size_t op_index = default_op_index)
    {
        std::lock_guard lock(mu);
        RUNTIME_CHECK(op_index < build_side_spilling_tasks.size());
        if constexpr (is_build_side)
        {
            auto & spilling_tasks = build_side_spilling_tasks[op_index];
            RUNTIME_CHECK(spilling_tasks > 0);
            --spilling_tasks;
        }
        else
        {
            auto & spilling_tasks = probe_side_spilling_tasks[op_index];
            RUNTIME_CHECK(spilling_tasks > 0);
            --spilling_tasks;
        }
    }

private:
    PipelineExecutorContext & exec_context;

    JoinPtr join;

    std::mutex mu;

    static constexpr auto default_op_index = 0;

    std::vector<UInt64> build_side_spilling_tasks;
    std::vector<UInt64> probe_side_spilling_tasks;
};
using JoinSpillContextPtr = std::shared_ptr<JoinSpillContext>;
} // namespace DB

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
#include <Common/Logger.h>
#include <Storages/KVStore/Utils.h>

#include <atomic>
#include <memory>
#include <unordered_map>

namespace DB
{
enum class PrehandleTransformStatus
{
    Ok,
    Aborted,
    ErrUpdateSchema,
    ErrTableDropped,
};

struct PreHandlingTrace : private MutexLockWrap
{
    struct Item
    {
        Item()
            : abort_error(PrehandleTransformStatus::Ok)
        {}
        bool isAbort() const { return abort_error.load() != PrehandleTransformStatus::Ok; }
        std::optional<PrehandleTransformStatus> abortReason() const
        {
            auto res = abort_error.load();
            if (res == PrehandleTransformStatus::Ok)
            {
                return std::nullopt;
            }
            return res;
        }
        void abortFor(PrehandleTransformStatus reason) { abort_error.store(reason); }
        void reset() { abort_error.store(PrehandleTransformStatus::Ok); }

    protected:
        std::atomic<PrehandleTransformStatus> abort_error;
    };

    // Prehandle use thread pool from Proxy's side, so it can't benefit from AsyncTasks.
    std::unordered_map<uint64_t, std::shared_ptr<Item>> tasks;
    std::atomic<uint64_t> ongoing_prehandle_subtask_count{0};
    std::mutex cpu_resource_mut;
    std::condition_variable cpu_resource_cv;
    LoggerPtr log;

    PreHandlingTrace()
        : log(Logger::get("PreHandlingTrace"))
    {}
    std::shared_ptr<Item> registerTask(uint64_t region_id);
    std::shared_ptr<Item> deregisterTask(uint64_t region_id);
    bool hasTask(uint64_t region_id);
    void waitForSubtaskResources(uint64_t region_id, size_t parallel, size_t parallel_subtask_limit);
    void releaseSubtaskResources(uint64_t region_id, size_t split_id)
    {
        std::unique_lock<std::mutex> cpu_resource_lock(cpu_resource_mut);
        // TODO(split) refine this to avoid notify_all
        auto prev = ongoing_prehandle_subtask_count.fetch_sub(1);
        RUNTIME_CHECK_MSG(
            prev > 0,
            "Try to decrease prehandle subtask count to below 0, region_id={}, split_id={}",
            region_id,
            split_id);
        cpu_resource_cv.notify_all();
    }
};
} // namespace DB

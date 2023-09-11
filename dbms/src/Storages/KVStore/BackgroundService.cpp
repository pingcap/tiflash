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

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/KVStore/BackgroundService.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/Page/PageConstants.h>

namespace DB
{
BackgroundService::BackgroundService(TMTContext & tmt_)
    : tmt(tmt_)
    , background_pool(tmt.getContext().getBackgroundPool())
    , log(Logger::get())
{
    RUNTIME_CHECK_MSG(tmt.isInitialized(), "TMTContext is not initialized");

    auto & global_context = tmt.getContext();
    if (!global_context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        // compute node does not contains region
        single_thread_task_handle = background_pool.addTask(
            [this] {
                tmt.getKVStore()->gcPersistedRegion();
                return false;
            },
            false,
            /*interval_ms=*/5 * 60 * 1000);

        // compute node does not contain long-live tables and segments
        auto & global_settings = global_context.getSettingsRef();
        storage_gc_handle = background_pool.addTask(
            [this] { return tmt.getGCManager().work(); },
            false,
            /*interval_ms=*/global_settings.dt_bg_gc_check_interval * 1000);
        LOG_INFO(
            log,
            "Start background storage gc worker with interval {} seconds.",
            global_settings.dt_bg_gc_check_interval);

        if (global_context.getPageStorageRunMode() == PageStorageRunMode::UNI_PS)
        {
            eager_raft_log_gc_handle = background_pool.addTask(
                [this] {
                    auto kvstore = tmt.getKVStore();
                    if (auto hints = kvstore->getRaftLogGcHints(); !hints.empty())
                    {
                        auto task_res = executeRaftLogGcTasks(tmt.getContext(), std::move(hints));
                        kvstore->applyRaftLogGcTaskRes(task_res);
                        // If some Regions have execute eager gc, then run again immediatly.
                        // Else run at fixed interval.
                        return !task_res.empty();
                    }
                    // no tasks, run at fixed interval
                    return false;
                },
                false,
                /*interval_ms=*/60 * 1000);
        }
    }
}

void BackgroundService::shutdown() noexcept
{
    if (single_thread_task_handle)
    {
        background_pool.removeTask(single_thread_task_handle);
        single_thread_task_handle = nullptr;
    }

    if (storage_gc_handle)
    {
        background_pool.removeTask(storage_gc_handle);
        storage_gc_handle = nullptr;
    }

    if (eager_raft_log_gc_handle)
    {
        background_pool.removeTask(eager_raft_log_gc_handle);
        eager_raft_log_gc_handle = nullptr;
    }
}

BackgroundService::~BackgroundService()
{
    shutdown();
}

} // namespace DB

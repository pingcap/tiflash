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

#include <Interpreters/Context.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshotManager.h>

namespace DB::DM::Remote
{

DisaggSnapshotManager::DisaggSnapshotManager(Context & ctx)
    : global_ctx(ctx.getGlobalContext())
    , log(Logger::get())
{
    handle = global_ctx.getBackgroundPool().addTask([&] {
        this->clearExpiredSnapshots();
        return false;
    });
}

DisaggSnapshotManager::~DisaggSnapshotManager()
{
    if (handle)
    {
        global_ctx.getBackgroundPool().removeTask(handle);
        handle = nullptr;
    }
}


void DisaggSnapshotManager::clearExpiredSnapshots()
{
    std::unique_lock lock(mtx);
    Timepoint now = Clock::now();
    for (auto iter = snapshots.begin(); iter != snapshots.end(); /*empty*/)
    {
        if (iter->second.expired_at < now)
        {
            LOG_INFO(log, "Remove expired Disaggregated Snapshot, task_id={} expired_at={:%Y-%m-%d %H:%M:%S}", iter->first, iter->second.expired_at);
            iter = snapshots.erase(iter);
        }
        else
        {
            ++iter;
        }
    }
}

} // namespace DB::DM::Remote

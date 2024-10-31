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

#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager.h>

namespace DB::DM::Remote
{
WNDisaggSnapshotManager::WNDisaggSnapshotManager(BackgroundProcessingPool & bg_pool)
    : pool(bg_pool)
    , log(Logger::get())
{
    handle = pool.addTask([&] {
        this->clearExpiredSnapshots();
        return false;
    });
}

WNDisaggSnapshotManager::~WNDisaggSnapshotManager()
{
    if (handle)
    {
        pool.removeTask(handle);
        handle = nullptr;
    }
}

bool WNDisaggSnapshotManager::unregisterSnapshotIfEmpty(const DisaggTaskId & task_id)
{
    auto snap = getSnapshot(task_id);
    if (!snap)
        return false;
    if (!snap->empty())
        return false;
    return unregisterSnapshot(task_id);
}

void WNDisaggSnapshotManager::clearExpiredSnapshots()
{
    std::unique_lock lock(mtx);
    Timepoint now = Clock::now();
    for (auto iter = snapshots.begin(); iter != snapshots.end(); /*empty*/)
    {
        if (iter->second.expired_at < now)
        {
<<<<<<< HEAD
            LOG_INFO(
                log,
                "Remove expired Disaggregated Snapshot, task_id={} expired_at={:%Y-%m-%d %H:%M:%S}",
                iter->first,
                iter->second.expired_at);
            iter = snapshots.erase(iter);
=======
            if (iter->second->expired_at < now)
            {
                LOG_INFO(
                    log,
                    "Remove expired Disaggregated Snapshot, task_id={} expired_at={:%Y-%m-%d %H:%M:%S}",
                    iter->first,
                    iter->second->expired_at);
                iter = snapshots.erase(iter);
            }
            else
            {
                ++iter;
            }
>>>>>>> 5dd3a733a2 (Disagg: refresh expiration time of snapshot when calling getSnapshot (#9570))
        }
        else
        {
            ++iter;
        }
    }
}

} // namespace DB::DM::Remote

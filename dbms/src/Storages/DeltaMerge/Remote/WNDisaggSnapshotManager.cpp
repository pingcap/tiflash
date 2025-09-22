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

DisaggReadSnapshotPtr WNDisaggSnapshotManager::getDisaggSnapshot(
    const DisaggTaskId & task_id,
    std::optional<std::chrono::seconds> refresh_duration) const
{
    return snapshots.withShared([&](auto & snapshots) {
        if (auto iter = snapshots.find(task_id); iter != snapshots.end())
        {
            if (refresh_duration.has_value())
            {
                if (bool done_refresh = iter->second->refreshExpiredTime(refresh_duration.value()); done_refresh)
                    LOG_INFO(
                        log,
                        "Refresh Disaggregated Snapshot expiration, task_id={} refresh_duration={}",
                        task_id,
                        refresh_duration.value());
            }
            return iter->second->snap;
        }
        return DisaggReadSnapshotPtr{nullptr};
    });
}

bool WNDisaggSnapshotManager::unregisterSnapshotIfEmpty(const DisaggTaskId & task_id)
{
    auto snap = getDisaggSnapshot(task_id, /*refresh_duration*/ std::nullopt);
    if (!snap)
        return false;
    if (!snap->empty())
        return false;
    return unregisterSnapshot(task_id);
}

bool WNDisaggSnapshotManager::unregisterSnapshot(const DisaggTaskId & task_id)
{
    return snapshots.withExclusive([&](auto & snapshots) {
        if (auto iter = snapshots.find(task_id); iter != snapshots.end())
        {
            LOG_INFO(log, "Unregister Disaggregated Snapshot, task_id={}", task_id);
            snapshots.erase(iter);
            return true;
        }
        return false;
    });
}

size_t WNDisaggSnapshotManager::getActiveSnapshotCount() const
{
    return snapshots.withShared([&](auto & snapshots) { return snapshots.size(); });
}

void WNDisaggSnapshotManager::clearExpiredSnapshots()
{
    Timepoint now = Clock::now();
    snapshots.withExclusive([&](auto & snapshots) {
        for (auto iter = snapshots.begin(); iter != snapshots.end(); /*empty*/)
        {
            auto exp = iter->second->getExpiredAt();
            if (exp < now)
            {
                LOG_INFO(
                    log,
                    "Remove expired Disaggregated Snapshot, task_id={} expired_at={:%Y-%m-%d %H:%M:%S}",
                    iter->first,
                    exp);
                iter = snapshots.erase(iter);
            }
            else
            {
                ++iter;
            }
        }
    });
}

} // namespace DB::DM::Remote

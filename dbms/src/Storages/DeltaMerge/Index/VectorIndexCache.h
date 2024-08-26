// Copyright 2024 PingCAP, Inc.
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

#include <Common/LRUCache.h>
#include <Storages/DeltaMerge/Index/VectorIndex_fwd.h>
#include <common/types.h>

#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_set>

namespace DB::DM::tests
{
class VectorIndexTestUtils;
}

namespace DB::DM
{

class VectorIndexCache
{
private:
    using Cache = LRUCache<String, VectorIndexViewer>;

    Cache cache;
    LoggerPtr log;

    // Note: Key exists if cache does internal eviction. However it is fine, because
    // we will remove them periodically.
    std::unordered_set<String> files_to_check;
    std::shared_mutex mu;

    std::atomic<bool> is_shutting_down = false;
    std::condition_variable shutdown_cv;
    std::mutex shutdown_mu;

private:
    friend class ::DB::DM::tests::VectorIndexTestUtils;

    // Drop the in-memory Vector Index if the on-disk file is deleted.
    // mmaped file could be unmmaped so that disk space can be reclaimed.
    size_t cleanOutdatedCacheEntries();

    void cleanOutdatedLoop();

    // TODO(vector-index): Use task on BackgroundProcessingPool instead of a raw thread
    std::thread cleaner_thread;

public:
    explicit VectorIndexCache(size_t max_entities);

    ~VectorIndexCache();

    template <typename LoadFunc>
    Cache::MappedPtr getOrSet(const Cache::Key & file_path, LoadFunc && load)
    {
        {
            std::scoped_lock lock(mu);
            files_to_check.insert(file_path);
        }

        auto result = cache.getOrSet(file_path, load);
        return result.first;
    }
};

} // namespace DB::DM

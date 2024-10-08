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

#include <Poco/File.h>
#include <Storages/DeltaMerge/Index/VectorIndexCache.h>

#include <mutex>

namespace DB::DM
{

size_t VectorIndexCache::cleanOutdatedCacheEntries()
{
    size_t cleaned = 0;

    std::unordered_set<String> files;
    {
        // Copy out the list to avoid occupying lock for too long.
        // The complexity is just O(N) which is fine.
        std::shared_lock lock(mu);
        files = files_to_check;
    }

    for (const auto & file_path : files)
    {
        if (is_shutting_down)
            break;

        if (!cache.contains(file_path))
        {
            // It is evicted from LRU cache
            std::unique_lock lock(mu);
            files_to_check.erase(file_path);
        }
        else if (!Poco::File(file_path).exists())
        {
            LOG_INFO(log, "Dropping in-memory Vector Index cache because on-disk file is dropped, file={}", file_path);
            {
                std::unique_lock lock(mu);
                files_to_check.erase(file_path);
            }
            cache.remove(file_path);
            cleaned++;
        }
    }

    LOG_DEBUG(log, "Cleaned {} outdated Vector Index cache entries", cleaned);

    return cleaned;
}

void VectorIndexCache::cleanOutdatedLoop()
{
    while (true)
    {
        {
            std::unique_lock lock(shutdown_mu);
            shutdown_cv.wait_for(lock, std::chrono::minutes(1), [this] { return is_shutting_down.load(); });
        }

        if (is_shutting_down)
            break;

        try
        {
            cleanOutdatedCacheEntries();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

VectorIndexCache::VectorIndexCache(size_t max_entities)
    : cache(max_entities)
    , log(Logger::get())
{
    cleaner_thread = std::thread([this] { cleanOutdatedLoop(); });
}

VectorIndexCache::~VectorIndexCache()
{
    is_shutting_down = true;
    shutdown_cv.notify_all();
    cleaner_thread.join();
}

} // namespace DB::DM

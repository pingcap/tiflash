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

#include <Common/CurrentMetrics.h>
#include <Common/TiFlashMetrics.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>

namespace CurrentMetrics
{
extern const Metric PageCacheCapacity;
extern const Metric PageCacheUsed;
} // namespace CurrentMetrics

namespace DB::DM::Remote
{

RNLocalPageCache::RNLocalPageCache(const RNLocalPageCacheOptions & options)
    : log(Logger::get())
    , storage(options.underlying_storage)
    , max_size(options.max_size_bytes)
    , evictable_keys(max_size)
{
    RUNTIME_CHECK(storage != nullptr);

    if (max_size > 0)
    {
        CurrentMetrics::set(CurrentMetrics::PageCacheCapacity, max_size);
        // Initially all PS entries are evictable.
        storage->traverseEntries("", [&](UniversalPageId page_id, DB::PageEntry entry) {
            evictable_keys.put(page_id, entry.size);
        });

        std::unique_lock lock(mu);
        LOG_DEBUG(log, "Initialized local page cache from existing PS, stats={}", statistics(lock));
    }
    else
    {
        LOG_WARNING(
            log,
            "Max capacity is not configured for local page cache. This may cause disk being filled quickly.");
    }
}

void RNLocalPageCache::write(
    const PageOID & oid,
    const ReadBufferPtr & read_buffer,
    PageSize size,
    const PageFieldSizes & field_sizes)
{
    auto key = buildCacheId(oid);

    if (max_size > 0)
    {
        std::unique_lock lock(mu);
        RUNTIME_CHECK_MSG(
            occupied_keys.find(key) != occupied_keys.end(),
            "Page {} was not occupied before writing",
            key);
        RUNTIME_CHECK_MSG(
            occupied_keys[key].size == size,
            "Page {} write size is different to occupy size, write_size={} occupy_size={}",
            key,
            size,
            occupied_keys[key].size);
    }

    UniversalWriteBatch cache_wb;
    cache_wb.putPage(key, 0, read_buffer, size, field_sizes);
    GET_METRIC(tiflash_storage_remote_cache, type_page_download).Increment();
    GET_METRIC(tiflash_storage_remote_cache_bytes, type_page_download_bytes).Increment(size);
    storage->write(std::move(cache_wb));
}

void RNLocalPageCache::write(const PageOID & oid, std::string_view data, const PageFieldSizes & field_sizes)
{
    auto read_buf = std::make_shared<ReadBufferFromOwnString>(data);
    write(oid, read_buf, data.size(), field_sizes);
}

void RNLocalPageCache::write(UniversalWriteBatch && wb)
{
    if (max_size > 0)
    {
        const auto & writes = wb.getWrites();
        std::unique_lock lock(mu);
        for (const auto & w : writes)
        {
            const auto & itr = occupied_keys.find(w.page_id);
            RUNTIME_CHECK_MSG(itr != occupied_keys.end(), "Page {} was not occupied before writing", w.page_id);
            RUNTIME_CHECK_MSG(
                itr->second.size == w.size,
                "Page {} write size is different to occupy size, write_size={} occupy_size={}",
                w.page_id,
                w.size,
                itr->second.size);
        }
    }
    GET_METRIC(tiflash_storage_remote_cache, type_page_download).Increment(wb.getWrites().size());
    GET_METRIC(tiflash_storage_remote_cache_bytes, type_page_download_bytes).Increment(wb.getTotalDataSize());
    storage->write(std::move(wb));
}

Page RNLocalPageCache::getPage(const PageOID & oid, const std::vector<size_t> & indices)
{
    auto key = buildCacheId(oid);

    if (max_size > 0)
    {
        std::unique_lock lock(mu);
        RUNTIME_CHECK_MSG(
            occupied_keys.find(key) != occupied_keys.end(),
            "Page {} was not occupied before reading",
            key);
    }

    auto snapshot = storage->getSnapshot("RNLocalPageCache.getPage");
    auto page_map = storage->read(
        {{key, indices}},
        /* read_limiter */ nullptr,
        snapshot,
        /* throw_on_not_exist */ true);
    auto page = page_map.at(key);

    RUNTIME_CHECK(page.isValid());
    GET_METRIC(tiflash_storage_remote_cache_bytes, type_page_read_bytes).Increment(page.data.size());
    return page;
}

void RNLocalPageCache::evictFromStorage(std::unique_lock<std::mutex> &)
{
    // Note: Not all keys in the `evictable_keys` will be evicted.
    // We will only evict overflow keys.

#ifndef NDEBUG
    // Integrity check: verify `occupied_size` is correct.
    {
        size_t occupied_size_2 = 0;
        for (auto & iter : occupied_keys)
            occupied_size_2 += iter.second.size;
        RUNTIME_CHECK(occupied_size_2 == occupied_size, occupied_size, occupied_size_2);
    }
#endif
    RUNTIME_CHECK(occupied_size <= max_size, occupied_size, max_size);
    evictable_keys.setMaxSize(max_size - occupied_size);
    auto evicted_keys = evictable_keys.evict();
    if (!evicted_keys.empty())
    {
        UniversalWriteBatch wb_del;
        for (const auto & key : evicted_keys)
            wb_del.delPage(key);
        storage->write(std::move(wb_del));
    }
}

void RNLocalPageCache::guard(
    std::unique_lock<std::mutex> & lock,
    const std::vector<UniversalPageId> & keys,
    const std::vector<size_t> & sizes,
    uint64_t guard_debug_id)
{
    RUNTIME_CHECK(max_size > 0);

    RUNTIME_CHECK(keys.size() == sizes.size(), keys.size(), sizes.size());
    const size_t n = keys.size();

    size_t newly_occupied_n = 0; // Only for debug output
    size_t newly_occupied_size = 0; // Only for debug output
    size_t total_keys_size = 0; // Only for debug output

    for (size_t i = 0; i < n; ++i)
    {
        auto place_result = occupied_keys.try_emplace(keys[i], OccupyInfo{});
        auto & info = place_result.first->second;
        bool inserted = place_result.second;

        total_keys_size += sizes[i];

        if (inserted)
        {
            newly_occupied_n += 1;
            newly_occupied_size += sizes[i];
            occupied_size += sizes[i];

            info.size = sizes[i];
            info.alive_guards = 1;

            LOG_TRACE(log, "Occupy: key={} size={} stats={}", keys[i], sizes[i], statistics(lock));

            // Keep these keys not evictable.
            // Only necessary when keys are added to the `occupied_keys` for the first time.
            evictable_keys.remove(keys[i]);
        }
        else
        {
            // TODO: Check size doesn't change.
            info.alive_guards += 1;
        }
    }

    LOG_DEBUG(
        log,
        "Guard keys, guard={} size={}(n={}) newly_occupied_size={}(n={}) stats={}",
        guard_debug_id,
        total_keys_size,
        keys.size(),
        newly_occupied_size,
        newly_occupied_n,
        statistics(lock));

    evictFromStorage(lock);
}

void RNLocalPageCache::unguard(const std::vector<UniversalPageId> & keys, uint64_t guard_debug_id)
{
    RUNTIME_CHECK(max_size > 0);

    std::unique_lock lock(mu);

    size_t released_occupied_n = 0; // Only for debug output
    size_t released_occupied_size = 0; // Only for debug output

    for (const auto & key : keys)
    {
        auto it = occupied_keys.find(key);
        RUNTIME_CHECK(it != occupied_keys.end());

        it->second.alive_guards -= 1;

        // Mark key as evictable, when it is not occupied any more.
        // Evictable key may be evicted immediately (if space is insufficient), or evicted in future.
        if (it->second.alive_guards == 0)
        {
            size_t size = it->second.size;

            released_occupied_n += 1;
            released_occupied_size += size;

            evictable_keys.put(key, size);
            occupied_keys.erase(it);
            occupied_size -= size;
        }
    }

    LOG_DEBUG(
        log,
        "Unguard keys, guard={} n={} released_occupied_size={}(n={}) stats={}",
        guard_debug_id,
        keys.size(),
        released_occupied_size,
        released_occupied_n,
        statistics(lock));

    evictFromStorage(lock);

    cv.notify_all();
}

RNLocalPageCache::OccupySpaceResult RNLocalPageCache::occupySpace(
    const std::vector<PageOID> & pages,
    const std::vector<size_t> & page_sizes,
    ScanContextPtr scan_context)
{
    RUNTIME_CHECK(pages.size() == page_sizes.size(), pages.size(), page_sizes.size());
    const size_t n = pages.size();

    std::vector<UniversalPageId> keys;
    keys.reserve(n);
    for (const auto & page : pages)
        keys.emplace_back(buildCacheId(page));

    RNLocalPageCacheGuardPtr guard{};

    if (max_size > 0)
    {
        std::unique_lock lock(mu);

        size_t need_occupy_size = 0;
        auto update_need_occupy_size = [&] {
            need_occupy_size = 0;
            for (size_t i = 0; i < n; ++i)
            {
                auto it = occupied_keys.find(keys[i]);
                if (it == occupied_keys.end())
                    need_occupy_size += page_sizes[i];
            }
        };

        update_need_occupy_size();
        if (need_occupy_size > max_size)
            throw Exception(
                fmt::format("Occupy space failed, max_size={} need_occupy_size={}", max_size, need_occupy_size));

        if (occupied_size + need_occupy_size > max_size)
        {
            LOG_WARNING(
                log,
                "Start waiting because local page cache space is insufficient to contain living query data, "
                "need_occupy_size={} all_keys_n={} stats={}",
                need_occupy_size,
                n,
                statistics(lock));

            Stopwatch watch;

            // There are too many occupies, wait...

            // Sum of wait_seconds is 1+2+4+8+16+32+64+64+64 = 255 seconds.
            // Since the default maximum lifetime of snapshot in WriteNodes is 300 seconds, waiting for more than the maximum lifetime is meaningless.
            constexpr std::array<Int32, 9> wait_seconds = {1, 2, 4, 8, 16, 32, 64, 64, 64};
            bool succ = false;
            for (int wait_second : wait_seconds)
            {
                GET_METRIC(tiflash_storage_remote_cache, type_page_full).Increment();
                auto cv_status = cv.wait_for(lock, std::chrono::seconds(wait_second));
                if (cv_status == std::cv_status::timeout)
                    LOG_WARNING(
                        log,
                        "Still waiting local page cache to release space, elapsed={}s need_occupy_size={} "
                        "all_keys_n={} stats={}",
                        watch.elapsedSeconds(),
                        need_occupy_size,
                        n,
                        statistics(lock));

                // Some keys may be occupied, so that need_occupy_size may be changed.
                update_need_occupy_size();
                if (succ = occupied_size + need_occupy_size <= max_size; succ)
                    break;
            }

            RUNTIME_CHECK_MSG(
                succ,
                "PageStorage cache space is insufficient to contain living query data. occupied_size={}, "
                "need_occupy_size={}, max_size={}",
                occupied_size,
                need_occupy_size,
                max_size);

            LOG_WARNING(
                log,
                "Finished waiting local page cache to release space, elapsed={}s need_occupy_size={} all_keys_n={} "
                "stats={}",
                watch.elapsedSeconds(),
                need_occupy_size,
                n,
                statistics(lock));
        }
        else
        {
            LOG_DEBUG(
                log,
                "Occupy space without waiting, need_occupy_size={} all_keys_n={} stats={}",
                need_occupy_size,
                n,
                statistics(lock));
        }

        // Guard keys first, then check existence. In this way, these keys will not be
        // evicted after the check.
        auto this_ptr = shared_from_this();
        guard = std::make_shared<RNLocalPageCacheGuard>(this_ptr, lock, keys, page_sizes);
    }
    else
    {
        // When max_size == 0, let's keep guard == nullptr, so that no keys in the LRU will be
        // touched.
        RUNTIME_CHECK(guard == nullptr);
    }

    auto snapshot = storage->getSnapshot("RNLocalPageCache.occupySpace");
    std::vector<PageOID> missing_ids;
    for (size_t i = 0; i < n; ++i)
    {
        // Pages may be occupied but not written yet, so we always return missing pages according
        // to the storage.
        if (const auto & page_entry = storage->getEntry(keys[i], snapshot); page_entry.isValid())
        {
            scan_context->disagg_read_cache_hit_size += page_sizes[i];
            continue;
        }
        missing_ids.push_back(pages[i]);
        scan_context->disagg_read_cache_miss_size += page_sizes[i];
    }
    GET_METRIC(tiflash_storage_remote_cache, type_page_miss).Increment(missing_ids.size());
    GET_METRIC(tiflash_storage_remote_cache, type_page_hit).Increment(pages.size() - missing_ids.size());
    return OccupySpaceResult{
        .pages_not_in_cache = missing_ids,
        .pages_guard = guard,
    };
}

bool RNLocalPageCacheLRU::put(const UniversalPageId & key, size_t size)
{
    if (max_size == 0)
        return false;

    auto place_result = index.try_emplace(key, Item{});
    auto & item = place_result.first->second;
    bool inserted = place_result.second;

    if (inserted)
    {
        item.size = size;
        item.queue_iter = queue.insert(queue.end(), key);
        current_total_size += size;
    }
    else
    {
        RUNTIME_CHECK_MSG(
            size == item.size,
            "Put an item with different size, new_size={} old_size={}",
            size,
            item.size);
        queue.splice(queue.end(), queue, item.queue_iter);
    }

    LOG_TRACE(log, "LRU put {} size={} lru={}", key, size, statistics());
    CurrentMetrics::set(CurrentMetrics::PageCacheUsed, current_total_size);

    return inserted;
}

bool RNLocalPageCacheLRU::remove(const UniversalPageId & key)
{
    auto it = index.find(key);
    if (it == index.end())
        return false;

    current_total_size -= it->second.size;
    queue.erase(it->second.queue_iter);
    index.erase(it);
    CurrentMetrics::set(CurrentMetrics::PageCacheUsed, current_total_size);

    return true;
}

std::vector<UniversalPageId> RNLocalPageCacheLRU::evict()
{
    if (current_total_size <= max_size)
        return {};

    std::vector<UniversalPageId> evicted;
    size_t evicted_bytes = 0;
    while (current_total_size > max_size && queue.size() > 1)
    {
        const auto & key = queue.front();

        auto it = index.find(key);
        RUNTIME_CHECK(it != index.end());

        evicted.emplace_back(key);
        LOG_TRACE(log, "LRU evict, key={} size={}", key, it->second.size);

        current_total_size -= it->second.size;
        evicted_bytes += it->second.size;
        queue.pop_front();
        index.erase(it);
    }
    GET_METRIC(tiflash_storage_remote_cache, type_page_evict).Increment(evicted.size());
    GET_METRIC(tiflash_storage_remote_cache_bytes, type_page_evict_bytes).Increment(evicted_bytes);
    CurrentMetrics::set(CurrentMetrics::PageCacheUsed, current_total_size);

    LOG_DEBUG(log, "LRU evict finished, lru={}", statistics());

#ifndef NDEBUG
    // Integrity check: verify `total_size` is correct.
    {
        size_t total_size = 0;

        RUNTIME_CHECK(queue.size() == index.size(), queue.size(), index.size());

        for (const auto & key : queue)
        {
            const auto it = index.find(key);
            RUNTIME_CHECK(it != index.end());
            total_size += it->second.size;
        }
        RUNTIME_CHECK(total_size == current_total_size, total_size, current_total_size);
    }
#endif

    return evicted;
}

} // namespace DB::DM::Remote

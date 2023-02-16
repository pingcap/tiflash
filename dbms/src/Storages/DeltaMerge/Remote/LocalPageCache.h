// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Logger.h>
#include <Storages/DeltaMerge/Remote/LocalPageCache_fwd.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/universal/UniversalPageStorage.h>

#include <boost/noncopyable.hpp>

namespace DB
{
class Context;
class ReadBuffer;
using ReadBufferPtr = std::shared_ptr<ReadBuffer>;

class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;
} // namespace DB

namespace DB::DM::Remote
{

class LocalPageCacheLRU
    : private boost::noncopyable
{
private:
    using Queue = std::list<UniversalPageId>;
    using QueueIter = typename Queue::iterator;

    struct Item
    {
        size_t size;
        QueueIter queue_iter;
    };

public:
    explicit LocalPageCacheLRU(size_t max_size_)
        : log(Logger::get())
        , max_size(max_size_)
    {
    }

    void setMaxSize(size_t max_size_)
    {
        max_size = max_size_;
    }

    String statistics() const
    {
        return fmt::format(
            "<n={} size={} max_size={}>",
            index.size(),
            current_total_size,
            max_size);
    }

    /// Returns true if the key is newly inserted.
    bool put(const UniversalPageId & key, size_t size)
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
            queue.splice(queue.end(), queue, item.queue_iter);
        }

        LOG_DEBUG(log, "LRU put {} size={} stats={}", key, size, statistics());

        return inserted;
    }

    /// Returns true if the key was existed and removed.
    bool remove(const UniversalPageId & key)
    {
        auto it = index.find(key);
        if (it == index.end())
            return false;

        queue.erase(it->second.queue_iter);
        index.erase(it);
        return true;
    }

    std::vector<UniversalPageId> evict()
    {
        if (current_total_size <= max_size)
            return {};

        std::vector<UniversalPageId> evicted;
        while (current_total_size > max_size && queue.size() > 1)
        {
            const auto & key = queue.front();

            auto it = index.find(key);
            RUNTIME_CHECK(it != index.end());

            evicted.emplace_back(key);
            LOG_DEBUG(log, "LRU evict {} size={}", key, it->second.size);

            current_total_size -= it->second.size;
            queue.pop_front();
            index.erase(it);
        }

        LOG_DEBUG(log, "LRU evict finished, stats={}", statistics());

        checkIntegrity();

        return evicted;
    }

private:
    void checkIntegrity()
    {
#ifndef NDEBUG
        size_t total_size = 0;

        RUNTIME_CHECK(queue.size() == index.size(), queue.size(), index.size());

        for (const auto & key : queue)
        {
            const auto it = index.find(key);
            RUNTIME_CHECK(it != index.end());
            total_size += it->second.size;
        }
        RUNTIME_CHECK(total_size == current_total_size, total_size, current_total_size);
#endif
    }

private:
    LoggerPtr log;

    size_t max_size;
    size_t current_total_size = 0;

    Queue queue;
    std::unordered_map<UniversalPageId, Item> index;
};

class LocalPageCache
    : public std::enable_shared_from_this<LocalPageCache>
    , private boost::noncopyable
{
    friend LocalPageCacheGuard;

private:
    void evictFromStorage(std::unique_lock<std::mutex> &)
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

    void guard(
        std::unique_lock<std::mutex> & lock,
        const std::vector<UniversalPageId> keys,
        const std::vector<size_t> sizes)
    {
        RUNTIME_CHECK(keys.size() == sizes.size(), keys.size(), sizes.size());
        const size_t n = keys.size();

        for (size_t i = 0; i < n; ++i)
        {
            auto place_result = occupied_keys.try_emplace(keys[i], OccupyInfo{});
            auto & info = place_result.first->second;
            bool inserted = place_result.second;

            if (inserted)
            {
                occupied_size += sizes[i];

                info.size = sizes[i];
                info.alive_guards = 1;

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

        evictFromStorage(lock);
    }

    void unguard(const std::vector<UniversalPageId> keys)
    {
        std::unique_lock lock(mu);

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

                evictable_keys.put(key, size);
                occupied_keys.erase(it);
                occupied_size -= size;
            }
        }

        evictFromStorage(lock);

        cv.notify_all();
    }

public:
    explicit LocalPageCache(const Context & global_context_);

    struct OccupySpaceResult
    {
        std::vector<PageOID> pages_not_in_cache;
        LocalPageCacheGuardPtr pages_guard;
    };

    /**
     * Giving a list of pages and sizes, wait until the cache is empty enough to hold all of these pages.
     * A Guard will be returned. When Guard is alive, these pages are ensured to be accessible if they are
     * put into the cache later.
     */
    OccupySpaceResult occupySpace(const std::vector<PageOID> & pages, const std::vector<size_t> & page_sizes)
    {
        RUNTIME_CHECK(pages.size() == page_sizes.size(), pages.size(), page_sizes.size());
        const size_t n = pages.size();

        std::vector<UniversalPageId> keys;
        keys.reserve(n);
        for (const auto & page : pages)
            keys.emplace_back(buildCacheId(page));

        std::unique_lock lock(mu);
        size_t new_occupy_size = 0;
        for (size_t i = 0; i < n; ++i)
        {
            auto it = occupied_keys.find(keys[i]);
            if (it == occupied_keys.end())
                new_occupy_size += page_sizes[i];
        }

        if (new_occupy_size > max_size)
            throw Exception(fmt::format("Occupy space failed, max_size={} new_occupy_size={}", max_size, new_occupy_size));

        if (occupied_size + new_occupy_size > max_size)
        {
            // There are too many occupies, wait...
            cv.wait(lock, [&] {
                new_occupy_size = 0;
                for (size_t i = 0; i < n; ++i)
                {
                    auto it = occupied_keys.find(keys[i]);
                    if (it == occupied_keys.end())
                        new_occupy_size += page_sizes[i];
                }
                bool stop_waiting = occupied_size + new_occupy_size <= max_size;
                return stop_waiting;
            });
        }

        auto this_ptr = shared_from_this();
        auto guard = std::make_shared<LocalPageCacheGuard>(this_ptr, lock, keys, page_sizes);

        lock.release();

        auto snapshot = storage->getSnapshot("LocalPageCache.occupySpace");
        std::vector<PageOID> missing_ids;
        for (size_t i = 0; i < n; ++i)
        {
            // Pages may be occupied but not written yet, so we always return missing pages according
            // to the storage.
            if (const auto & page_entry = storage->getEntry(keys[i], snapshot); page_entry.isValid())
                continue;
            missing_ids.push_back(pages[i]);
        }
        return OccupySpaceResult{
            .pages_not_in_cache = missing_ids,
            .pages_guard = guard,
        };
    }

    void write(
        const PageOID & oid,
        ReadBufferPtr && read_buffer,
        PageSize size,
        PageFieldSizes && field_sizes);

    Page getPage(const PageOID & oid, const PageStorage::FieldIndices & indices);

private:
    static UniversalPageId buildCacheId(const PageOID & oid)
    {
        return fmt::format("{}_{}_{}", oid.write_node_id, oid.table_id, oid.page_id);
    }

private:
    struct OccupyInfo
    {
        size_t size;
        size_t alive_guards;
    };

    LoggerPtr log;

    UniversalPageStoragePtr storage;

    const size_t max_size; // 0 == LRU disabled

    std::mutex mu;
    std::unordered_map<UniversalPageId, OccupyInfo> occupied_keys;
    size_t occupied_size = 0; // Pre-calculated value from occupied_keys.
    std::condition_variable cv;
    LocalPageCacheLRU evictable_keys;
};

class LocalPageCacheGuard
{
public:
    LocalPageCacheGuard(
        const std::shared_ptr<LocalPageCache> & parent_,
        std::unique_lock<std::mutex> & lock,
        const std::vector<UniversalPageId> keys_,
        const std::vector<size_t> sizes)
        : parent(parent_)
        , keys(keys_)
    {
        // Note: It is safe when a key occur multiple times in the `keys`,
        // as long as our `addPins` and `removePins` are matched.
        parent->guard(lock, keys, sizes);
    }

    ~LocalPageCacheGuard()
    {
        parent->unguard(keys);
    }

private:
    const std::shared_ptr<LocalPageCache> parent;
    const std::vector<UniversalPageId> keys;
};


} // namespace DB::DM::Remote

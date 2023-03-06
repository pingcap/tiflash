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
#include <Storages/Page/Page.h>
#include <Storages/Page/PageStorage_fwd.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>

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
            "<total_n={} total_size={} max_size={}>",
            index.size(),
            current_total_size,
            max_size);
    }

    /// Returns true if the key is newly inserted.
    bool put(const UniversalPageId & key, size_t size);

    /// Returns true if the key was existed and removed.
    bool remove(const UniversalPageId & key);

    std::vector<UniversalPageId> evict();

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
    String statistics(std::unique_lock<std::mutex> &) const
    {
        return fmt::format(
            "<occupied=<total_n={} total_size={} max_size={}> evictable={}>",
            occupied_keys.size(),
            occupied_size,
            max_size,
            evictable_keys.statistics());
    }

    void evictFromStorage(std::unique_lock<std::mutex> &);

    void guard(
        std::unique_lock<std::mutex> & lock,
        const std::vector<UniversalPageId> keys,
        const std::vector<size_t> sizes,
        uint64_t guard_debug_id);

    void unguard(const std::vector<UniversalPageId> keys, uint64_t guard_debug_id);

public:
    struct LocalPageCacheOptions
    {
        // TODO: May be better to manage the underlying storage by this module itself?
        UniversalPageStoragePtr underlying_storage;
        size_t max_size_bytes = 0; // 0 means unlimited.
    };

    explicit LocalPageCache(const LocalPageCacheOptions & options);

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
    OccupySpaceResult occupySpace(const std::vector<PageOID> & pages, const std::vector<size_t> & page_sizes);

    void write(
        const PageOID & oid,
        ReadBufferPtr && read_buffer,
        PageSize size,
        PageFieldSizes && field_sizes);

    Page getPage(const PageOID & oid, const std::vector<size_t> & indices);

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
        , debug_id(global_id_seq.fetch_add(1, std::memory_order_seq_cst))
    {
        // Note: It is safe when a key occur multiple times in the `keys`,
        // as long as our `addPins` and `removePins` are matched.
        parent->guard(lock, keys, sizes, debug_id);
    }

    ~LocalPageCacheGuard()
    {
        parent->unguard(keys, debug_id);
    }

private:
    const std::shared_ptr<LocalPageCache> parent;
    const std::vector<UniversalPageId> keys;
    const uint64_t debug_id; // Only for debug output

    inline static std::atomic<uint64_t> global_id_seq{1};
};

} // namespace DB::DM::Remote

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

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache_fwd.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageStorage_fwd.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>

#include <boost/noncopyable.hpp>

namespace DB
{
class ReadBuffer;
using ReadBufferPtr = std::shared_ptr<ReadBuffer>;

class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;
} // namespace DB

namespace DB::DM::Remote
{

/**
 * A standard LRU. Its max_size is changeable.
 *
 * It supports the following operations:
 * - Put:    Put a key into LRU's management.
 * - Remove: Remove a key from LRU's management. A removed key will not trigger
 *           any evict any more. Its size is also cleared.
 * - Evict:  Evict overflowed items, and return these items.
 *           Note that if an item was removed by calling `remove()`, it will not
 *           occur any more in the evicted list.
 *
 * Eviction only happens when you manually call `evict()`..
 */
class RNLocalPageCacheLRU : private boost::noncopyable
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
    explicit RNLocalPageCacheLRU(size_t max_size_)
        : log(Logger::get())
        , max_size(max_size_)
    {}

    void setMaxSize(size_t max_size_) { max_size = max_size_; }

    String statistics() const
    {
        return fmt::format("<total_n={} total_size={} max_size={}>", index.size(), current_total_size, max_size);
    }

    /// Returns true if the key is newly inserted.
    bool put(const UniversalPageId & key, size_t size);

    /// Returns true if the key was existed and removed.
    bool remove(const UniversalPageId & key);

    [[nodiscard]] std::vector<UniversalPageId> evict();

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    LoggerPtr log;

    size_t max_size;
    size_t current_total_size = 0;

    Queue queue;
    std::unordered_map<UniversalPageId, Item> index;
};


/**
 * Only used in disaggregated read node. It caches pages in a local PageStorage
 * instance to avoid repeatedly pulling page data from disaggregated write nodes.
 */
class RNLocalPageCache
    : public std::enable_shared_from_this<RNLocalPageCache>
    , private boost::noncopyable
{
    friend RNLocalPageCacheGuard;

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

    /**
     * Only called by RNLocalPageCacheGuard, when it is constructed.
     *
     * This function guards (pins) specified keys, avoid them from evicted.
     *
     * Internally, it removes these keys from the (evictable) LRU, to avoid being evicted.
     */
    void guard(
        std::unique_lock<std::mutex> & lock,
        const std::vector<UniversalPageId> & keys,
        const std::vector<size_t> & sizes,
        uint64_t guard_debug_id);

    /**
     * Only called by RNLocalPageCacheGuard, when it is destructed.
     *
     * This function unguards (unpins) specified keys, allowing them to be evicted, if there is no other guards
     * referencing the key.
     *
     * Internally, it adds these keys to the (evictable) LRU so that it could be evicted.
     */
    void unguard(const std::vector<UniversalPageId> & keys, uint64_t guard_debug_id);

public:
    struct RNLocalPageCacheOptions
    {
        // TODO: May be better to manage the underlying storage by this module itself?
        UniversalPageStoragePtr underlying_storage;
        size_t max_size_bytes = 0; // 0 means unlimited.
    };

    explicit RNLocalPageCache(const RNLocalPageCacheOptions & options);

    static RNLocalPageCachePtr create(const RNLocalPageCacheOptions & options)
    {
        return std::make_shared<RNLocalPageCache>(options);
    }

    struct OccupySpaceResult
    {
        std::vector<PageOID> pages_not_in_cache;
        RNLocalPageCacheGuardPtr pages_guard;
    };

    /**
     * Giving a list of pages and sizes, wait until the cache is empty enough to hold all of these pages.
     * A Guard will be returned. When Guard is alive, these pages are ensured to be accessible if they are
     * put into the cache later.
     *
     * This function also returns pages not in the cache.
     */
    OccupySpaceResult occupySpace(
        const std::vector<PageOID> & pages,
        const std::vector<size_t> & page_sizes,
        ScanContextPtr scan_context = nullptr);

    /**
     * Put a page into the cache.
     * The page must be currently "occupied" by calling `occupySpace`, otherwise there will be exceptions.
     */
    void write(
        const PageOID & oid,
        const ReadBufferPtr & read_buffer,
        PageSize size,
        const PageFieldSizes & field_sizes = {});

    /**
     * Put a page into the cache.
     * The page must be currently "occupied" by calling `occupySpace`, otherwise there will be exceptions.
     *
     * This is a shortcut function for write, only used in tests.
     */
    void write(const PageOID & oid, std::string_view data, const PageFieldSizes & field_sizes = {});

    void write(UniversalWriteBatch && wb);
    /**
     * Read a page from the cache.
     *
     * - The page must be currently "occupied" by calling `occupySpace`, otherwise there will be exceptions.
     * - The page must exist, either because `write` is called, or it is already in the cache,
     *   otherwise there will be exceptions.
     */
    Page getPage(const PageOID & oid, const std::vector<size_t> & indices);

    static UniversalPageId buildCacheId(const PageOID & oid)
    {
        if (oid.ks_table_id.first == NullspaceID)
        {
            return fmt::format("{}_{}_{}", oid.store_id, oid.ks_table_id.second, oid.page_id);
        }
        else
        {
            return fmt::format("{}_{}_{}_{}", oid.store_id, oid.ks_table_id.first, oid.ks_table_id.second, oid.page_id);
        }
    }

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
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
    RNLocalPageCacheLRU evictable_keys;
};

class RNLocalPageCacheGuard
{
public:
    RNLocalPageCacheGuard(
        const std::shared_ptr<RNLocalPageCache> & parent_,
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

    ~RNLocalPageCacheGuard() { parent->unguard(keys, debug_id); }

private:
    const std::shared_ptr<RNLocalPageCache> parent;
    const std::vector<UniversalPageId> keys;
    const uint64_t debug_id; // Only for debug output

    inline static std::atomic<uint64_t> global_id_seq{1};
};

} // namespace DB::DM::Remote

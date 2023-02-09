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

#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Remote/LocalPageCache.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/universal/UniversalPageStorage.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB::DM::Remote
{

// TODO: We should manage the ReadNodePageStorage inside LocalPageCache,
//       instead of relying on external creation.
LocalPageCache::LocalPageCache(const Context & global_context_)
    : lru_max_size(global_context_.getSettingsRef().local_page_cache_max_size)
    , log(Logger::get())
    , cache_storage(global_context_.getReadNodePageStorage())
    , global_context(global_context_)
{
    RUNTIME_CHECK(cache_storage != nullptr);

    cache_storage->traverseEntries([&](UniversalPageId page_id, DB::PageEntry entry) {
        lruPut(page_id, entry.size);
    });

    // LOG_DEBUG(log, "Initialized LRU from existing PS, current_size={} max_size={}", lru_current_size, lru_max_size);
}

void LocalPageCache::lruPut(const UniversalPageId & key, size_t size)
{
    if (!lruIsEnabled())
        return;

    std::unique_lock lock(lru_mu);

    auto place_result = lru_index.try_emplace(key, LRUElement{});
    auto & lru_element = place_result.first->second;
    bool inserted = place_result.second;

    if (inserted)
    {
        lru_element.size = size;
        lru_element.iter = lru_queue.insert(lru_queue.end(), key);
        lru_current_size += size;
    }
    else
    {
        lru_queue.splice(lru_queue.end(), lru_queue, lru_element.iter);
    }

    // LOG_DEBUG(log, "LRU put {}, size={} current_size={} max_size={}", key, size, lru_current_size, lru_max_size);
}

void LocalPageCache::lruRefresh(const UniversalPageId & key)
{
    if (!lruIsEnabled())
        return;

    std::unique_lock lock(lru_mu);

    auto it = lru_index.find(key);
    if (it == lru_index.end()) // Key not in LRU
        return;

    auto & lru_element = it->second;
    lru_queue.splice(lru_queue.end(), lru_queue, lru_element.iter);
}

std::vector<UniversalPageId> LocalPageCache::lruEvict()
{
    std::unique_lock lock(lru_mu);

    std::vector<UniversalPageId> evicted;

    if (lru_current_size < lru_max_size)
        return evicted;

    size_t queue_size = lru_index.size();
    while ((lru_current_size > lru_max_size) && (queue_size > 1))
    {
        const auto & key = lru_queue.front();

        auto it = lru_index.find(key);
        RUNTIME_CHECK(it != lru_index.end());

        evicted.emplace_back(key);
        // LOG_DEBUG(log, "LRU evict {}, size={}", key, it->second.size);

        lru_current_size -= it->second.size;
        --queue_size;
        lru_queue.pop_front();
        lru_index.erase(it);
    }

    // LOG_DEBUG(log, "LRU evict finished, current_size={} max_size={}", lru_current_size, lru_max_size);

    return evicted;
}

std::vector<PageOID> LocalPageCache::getMissingIds(const std::vector<PageOID> & pages)
{
    UNUSED(global_context);

    std::vector<PageOID> missing_ids;

    auto snapshot = cache_storage->getSnapshot("LocalPageCache.getMissingIds");
    for (const auto & page : pages)
    {
        auto cache_id = buildCacheId(page);
        if (const auto & page_entry = cache_storage->getEntry(cache_id, snapshot); page_entry.isValid())
            continue;
        missing_ids.push_back(page);
    }
    return missing_ids;
}

void LocalPageCache::write(
    const PageOID & oid,
    ReadBufferPtr && read_buffer,
    PageSize size,
    PageFieldSizes && field_sizes)
{
    if (lruIsEnabled() && size > lru_max_size)
        throw Exception(fmt::format(
            "Rejected large page exceeding local page cache limit, oid={} size={} lru_max_size={}",
            oid,
            size,
            lru_max_size));

    UniversalWriteBatch cache_wb;
    auto cache_id = buildCacheId(oid);
    cache_wb.putPage(cache_id, 0, read_buffer, size, field_sizes);
    cache_storage->write(std::move(cache_wb));

    lruPut(cache_id, size);
    auto evicted = lruEvict();
    if (!evicted.empty())
    {
        UniversalWriteBatch wb_del;
        for (const auto & id : evicted)
            wb_del.delPage(id);
        cache_storage->write(std::move(wb_del));
    }
}

Page LocalPageCache::getPage(const PageOID & oid, const PageStorage::FieldIndices & indices)
{
    // TODO: We should maintain this snapshot for longer time.
    auto snapshot = cache_storage->getSnapshot("LocalPageCache.getPage");

    auto cache_id = buildCacheId(oid);
    auto page_map = cache_storage->read(
        {{cache_id, indices}},
        /* read_limiter */ nullptr,
        snapshot,
        /* throw_on_not_exist */ false);
    auto page = page_map[cache_id];

    if (!page.isValid())
        throw Exception(fmt::format("Page is not valid, may be cleaned, oid={} cache_id={}", oid, cache_id));

    lruRefresh(cache_id);

    return page;
}


} // namespace DB::DM::Remote

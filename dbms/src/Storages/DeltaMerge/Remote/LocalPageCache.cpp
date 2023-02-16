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
    : log(Logger::get())
    , storage(global_context_.getReadNodePageStorage())
    , max_size(global_context_.getSettingsRef().local_page_cache_max_size)
    , evictable_keys(max_size)
{
    RUNTIME_CHECK(storage != nullptr);

    // Initially all PS entries are evictable.
    storage->traverseEntries([&](UniversalPageId page_id, DB::PageEntry entry) {
        evictable_keys.put(page_id, entry.size);
    });

    LOG_DEBUG(log, "Initialized LRU from existing PS, lru_stats={}", evictable_keys.statistics());
}

void LocalPageCache::write(
    const PageOID & oid,
    ReadBufferPtr && read_buffer,
    PageSize size,
    PageFieldSizes && field_sizes)
{
    {
        auto key = buildCacheId(oid);
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
    auto cache_id = buildCacheId(oid);
    cache_wb.putPage(cache_id, 0, read_buffer, size, field_sizes);
    storage->write(std::move(cache_wb));
}

Page LocalPageCache::getPage(const PageOID & oid, const PageStorage::FieldIndices & indices)
{
    auto key = buildCacheId(oid);

    {
        std::unique_lock lock(mu);
        RUNTIME_CHECK_MSG(
            occupied_keys.find(key) != occupied_keys.end(),
            "Page {} was not occupied before reading",
            key);
    }

    auto snapshot = storage->getSnapshot("LocalPageCache.getPage");
    auto page_map = storage->read(
        {{key, indices}},
        /* read_limiter */ nullptr,
        snapshot,
        /* throw_on_not_exist */ true);
    auto page = page_map[key];

    RUNTIME_CHECK(page.isValid());

    return page;
}

} // namespace DB::DM::Remote

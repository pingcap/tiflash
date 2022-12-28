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
    , cache_storage(global_context_.getReadNodePageStorage())
    , global_context(global_context_)
{
    RUNTIME_CHECK(cache_storage != nullptr);
}

#if 0
void LocalPageCache::ensurePagesReady(const std::vector<PageOID> & pages)
{
    auto snapshot = cache_storage->getSnapshot("LocalPageCache.ensurePages");

    const auto & table_storages = global_context.getTMTContext().getStorages();
    for (const auto & page : pages)
    {
        auto cache_id = buildCacheId(page);
        if (const auto & page_entry = cache_storage->getEntry(cache_id, snapshot); page_entry.isValid())
            continue;

        LOG_DEBUG(log, "Download page from remote, page_oid={}", page.info());

        // TODO: Send RPC to resolve page data
        auto store = table_storages.get(page.table_id);
        RUNTIME_CHECK(store != nullptr); // What if there are DDLs? Maybe we should throw non-logical errors.
        auto dm_store = std::dynamic_pointer_cast<StorageDeltaMerge>(store)->getStore();
        auto log_reader = dm_store->storage_pool->logReader();
        auto wn_page = log_reader->read(page.page_id);
        auto wn_entry = log_reader->getPageEntry(page.page_id);

        UniversalWriteBatch cache_wb;
        cache_wb.putPage(
            cache_id,
            0,
            std::make_shared<ReadBufferFromMemory>(wn_page.data.begin(), wn_page.data.size()),
            wn_page.data.size(),
            Page::fieldOffsetsToSizes(wn_entry.field_offsets, wn_entry.size));
        cache_storage->write(std::move(cache_wb));
    }
}
#endif

std::vector<PageOID> LocalPageCache::getPendingIds(const std::vector<PageOID> & pages)
{
    UNUSED(global_context);
    std::vector<PageOID> pending_ids;
    auto snapshot = cache_storage->getSnapshot("LocalPageCache.getPendingIds");
    for (const auto & page : pages)
    {
        auto cache_id = buildCacheId(page);
        if (const auto & page_entry = cache_storage->getEntry(cache_id, snapshot); page_entry.isValid())
            continue;

        pending_ids.push_back(page);
    }
    return pending_ids;
}

void LocalPageCache::write(
    const PageOID & oid,
    ReadBufferPtr && read_buffer,
    PageSize size,
    PageFieldSizes && field_sizes)
{
    UniversalWriteBatch cache_wb;
    auto cache_id = buildCacheId(oid);
    cache_wb.putPage(cache_id, 0, read_buffer, size, field_sizes);
    cache_storage->write(std::move(cache_wb));
}

Page LocalPageCache::getPage(const PageOID & oid, const PageStorage::FieldIndices & indices)
{
    // TODO
    auto snapshot = cache_storage->getSnapshot("LocalPageCache.getPage");

    auto cache_id = buildCacheId(oid);
    auto page_map = cache_storage->read({{cache_id, indices}}, /* read_limiter */ nullptr, snapshot);
    auto page = page_map[cache_id];
    RUNTIME_CHECK_MSG(page.isValid(), "Page {} is not valid, may be you forget to call ensurePages?");
    return page;
}


} // namespace DB::DM::Remote

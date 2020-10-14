#include <Functions/FunctionHelpers.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/Pack.h>
#include <Storages/DeltaMerge/DeltaValueSpace.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>

#include <ext/scope_guard.h>

namespace ProfileEvents
{
extern const Event DMWriteBytes;
}

namespace DB
{
namespace DM
{
const UInt64 DeltaValueSpace::CURRENT_VERSION = 2;

using Snapshot    = DeltaValueSpace::Snapshot;
using SnapshotPtr = std::shared_ptr<Snapshot>;

// ================================================
// Private methods
// ================================================

BlockPtr DeltaValueSpace::lastSchema()
{
    for (auto it = packs.rbegin(); it != packs.rend(); ++it)
    {
        if ((*it)->schema)
            return (*it)->schema;
    }
    return {};
}

void DeltaValueSpace::setUp()
{
    for (auto & pack : packs)
    {
        rows += pack->rows;
        bytes += pack->bytes;
        deletes += pack->isDeleteRange();
        if (!pack->isSaved())
        {
            unsaved_rows += pack->rows;
            unsaved_deletes += pack->isDeleteRange();
        }
    }
}

void DeltaValueSpace::checkNewPacks(const Packs & new_packs)
{
    if constexpr (!DM_RUN_CHECK)
        return;
    size_t new_rows    = 0;
    size_t new_deletes = 0;

    bool seen_unsaved = false;
    bool ok           = true;
    for (auto & pack : new_packs)
    {
        if (pack->isSaved() && seen_unsaved)
        {
            ok = false;
            break;
        }
        seen_unsaved |= !pack->isSaved();

        new_rows += pack->rows;
        new_deletes += pack->isDeleteRange();
    }
    if (unlikely(!ok || new_rows != rows || new_deletes != deletes))
    {
        LOG_ERROR(log,
                  "Rows and deletes check failed. Current packs: " << packsToString(packs) << ", new packs: " << packsToString(new_packs));
        throw Exception("Rows and deletes check failed.", ErrorCodes::LOGICAL_ERROR);
    }
}

// ================================================
// Public methods
// ================================================

DeltaValueSpace::DeltaValueSpace(PageId id_, bool is_common_handle_, size_t rowkey_column_size_, const Packs & packs_)
    : id(id_),
      packs(packs_),
      delta_index(std::make_shared<DeltaIndex>()),
      is_common_handle(is_common_handle_),
      rowkey_column_size(rowkey_column_size_),
      log(&Logger::get("DeltaValueSpace"))
{
    setUp();
}

void DeltaValueSpace::restore(DMContext & context)
{
    Page                 page = context.storage_pool.meta().read(id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    packs = deserializePacks(buf);

    setUp();
}

void DeltaValueSpace::saveMeta(WriteBatches & wbs) const
{
    MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
    // Only serialize saved packs.
    serializeSavedPacks(buf, packs);
    auto data_size = buf.count();
    wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

Packs DeltaValueSpace::checkHeadAndCloneTail(DMContext &         context,
                                             const RowKeyRange & target_range,
                                             const Packs &       head_packs,
                                             WriteBatches &      wbs) const
{
    if (head_packs.size() > packs.size())
    {
        LOG_ERROR(log,
                  info() << ", Delta  Check head packs failed, unexpected size. head_packs: " << packsToString(head_packs)
                         << ", packs: " << packsToString(packs));
        throw Exception("Check head packs failed, unexpected size", ErrorCodes::LOGICAL_ERROR);
    }

    auto it_1 = head_packs.begin();
    auto it_2 = packs.begin();
    for (; it_1 != head_packs.end() && it_2 != packs.end(); ++it_1, ++it_2)
    {
        if ((*it_1)->id != (*it_2)->id || (*it_1)->rows != (*it_2)->rows)
        {
            LOG_ERROR(log,
                      simpleInfo() << ", Delta  Check head packs failed, unexpected size. head_packs: " << packsToString(head_packs)
                                   << ", packs: " << packsToString(packs));
            throw Exception("Check head packs failed", ErrorCodes::LOGICAL_ERROR);
        }
    }

    Packs tail_clone;
    for (; it_2 != packs.end(); ++it_2)
    {
        // Clone a new pack, and column pages are referenced to the old.
        auto & pack     = *it_2;
        auto   new_pack = std::make_shared<Pack>(*pack);
        if (pack->isDeleteRange())
        {
            new_pack->delete_range = pack->delete_range.shrink(target_range);
            if (!new_pack->delete_range.none())
                tail_clone.push_back(new_pack);
        }
        else
        {
            if (pack->data_page != 0)
            {
                auto new_page_id = context.storage_pool.newLogPageId();
                wbs.log.putRefPage(new_page_id, pack->data_page);
                new_pack->data_page = new_page_id;
            }
            // No matter or what, don't append to packs which cloned from old packs again.
            // Because they could shared the same cache. And the cache can NOT be inserted from different packs in different delta.
            new_pack->appendable = false;
            tail_clone.push_back(new_pack);
        }
    }

    return tail_clone;
}

size_t DeltaValueSpace::getTotalCacheRows() const
{
    std::scoped_lock lock(mutex);
    size_t           cache_rows = 0;
    CachePtr         _last_cache;
    for (auto & pack : packs)
    {
        if (pack->cache && pack->cache != _last_cache)
        {
            cache_rows += pack->cache->block.rows();
        }
        _last_cache = pack->cache;
    }
    return cache_rows;
}

size_t DeltaValueSpace::getTotalCacheBytes() const
{
    std::scoped_lock lock(mutex);
    size_t           cache_bytes = 0;
    CachePtr         _last_cache;
    for (auto & pack : packs)
    {
        if (pack->cache && pack->cache != _last_cache)
        {
            cache_bytes += pack->cache->block.bytes();
        }
        _last_cache = pack->cache;
    }
    return cache_bytes;
}

size_t DeltaValueSpace::getValidCacheRows() const
{
    std::scoped_lock lock(mutex);
    size_t           cache_rows = 0;
    for (auto & pack : packs)
    {
        if (pack->isCached())
            cache_rows += pack->rows;
    }
    return cache_rows;
}

void DeltaValueSpace::recordRemovePacksPages(WriteBatches & wbs) const
{
    for (auto & pack : packs)
    {
        wbs.removed_log.delPage(pack->data_page);
    }
}

PageId DeltaValueSpace::writePackData(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs)
{
    auto page_id = context.storage_pool.newLogPageId();

    MemoryWriteBuffer write_buf;
    PageFieldSizes    col_data_sizes;
    for (auto & col : block)
    {
        auto last_buf_size = write_buf.count();
        serializeColumn(write_buf, *col.column, col.type, offset, limit, true);
        col_data_sizes.push_back(write_buf.count() - last_buf_size);
    }

    auto data_size = write_buf.count();
    auto buf       = write_buf.tryGetReadBuffer();
    wbs.log.putPage(page_id, 0, buf, data_size, col_data_sizes);

    return page_id;
}

PackPtr DeltaValueSpace::writePack(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs)
{
    auto pack       = std::make_shared<Pack>();
    pack->rows      = limit;
    pack->bytes     = block.bytes(offset, limit);
    pack->data_page = writePackData(context, block, offset, limit, wbs);
    pack->setSchema(std::make_shared<Block>(block.cloneEmpty()));
    pack->appendable = false;

    return pack;
}

bool DeltaValueSpace::appendToDisk(DMContext & /*context*/, const PackPtr & pack)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    auto last_schema = lastSchema();
    if (last_schema && checkSchema(*pack->schema, *last_schema))
        pack->schema = last_schema;

    if (!packs.empty())
        packs.back()->appendable = false;

    packs.push_back(pack);

    rows += pack->rows;
    bytes += pack->bytes;
    unsaved_rows += pack->rows;

    ProfileEvents::increment(ProfileEvents::DMWriteBytes, pack->bytes);

    return true;
}

bool DeltaValueSpace::appendToCache(DMContext & context, const Block & block, size_t offset, size_t limit)
{
    std::scoped_lock lock(mutex);

    if (abandoned.load(std::memory_order_relaxed))
        return false;

    // If last pack has a valid cache block, and it is mutable (haven't been saved to disk yet), we will merge the newly block into last pack.
    // Otherwise, create a new cache block and write into it.

    PackPtr mutable_pack{};
    if (!packs.empty())
    {
        auto & last_pack = packs.back();
        if (last_pack->isAppendable())
        {
            if constexpr (DM_RUN_CHECK)
            {
                if (unlikely(!checkSchema(*last_pack->schema, last_pack->cache->block)))
                    throw Exception("Mutable pack's structure of schema and block are different: " + last_pack->toString());
            }

            bool is_overflow    = last_pack->cache->block.rows() >= context.delta_cache_limit_rows;
            bool is_same_schema = checkSchema(block, last_pack->cache->block);
            if (!is_overflow && is_same_schema)
            {
                // The last cache block is available
                mutable_pack = last_pack;
            }
            else
            {
                last_pack->appendable = false;
            }
        }
    }

    auto append_data_to_cache = [&](const CachePtr & cache) {
        std::scoped_lock cache_lock(cache->mutex);

        for (size_t i = 0; i < cache->block.columns(); ++i)
        {
            auto & col               = block.getByPosition(i).column;
            auto & cache_col         = *cache->block.getByPosition(i).column;
            auto * mutable_cache_col = const_cast<IColumn *>(&cache_col);
            mutable_cache_col->insertRangeFrom(*col, offset, limit);
        }
    };

    size_t append_bytes = block.bytes(offset, limit);
    if (mutable_pack)
    {
        // Merge into last pack.
        mutable_pack->rows += limit;
        mutable_pack->bytes += append_bytes;

        append_data_to_cache(mutable_pack->cache);
    }
    else
    {
        // Create a new pack.
        auto pack   = std::make_shared<Pack>();
        pack->rows  = limit;
        pack->bytes = append_bytes;
        pack->cache = std::make_shared<Cache>(block);

        append_data_to_cache(pack->cache);

        auto last_schema = lastSchema();
        if (last_schema && checkSchema(block, *last_schema))
            pack->setSchema(last_schema);
        else
            pack->setSchema(std::make_shared<Block>(block.cloneEmpty()));

        packs.push_back(pack);
    }

    rows += limit;
    bytes += append_bytes;
    unsaved_rows += limit;

    return true;
}

bool DeltaValueSpace::appendDeleteRange(DMContext & /*context*/, const RowKeyRange & delete_range)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    auto pack          = std::make_shared<Pack>();
    pack->delete_range = delete_range;
    pack->appendable   = false;
    packs.push_back(pack);

    ++deletes;
    ++unsaved_deletes;

    return true;
}

} // namespace DM
} // namespace DB

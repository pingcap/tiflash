#include <Functions/FunctionHelpers.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/DeltaIndexManager.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace DM
{
// ================================================
// Private methods
// ================================================

BlockPtr DeltaValueSpace::lastSchema()
{
    for (auto it = packs.rbegin(); it != packs.rend(); ++it)
    {
        if (auto dp_block = (*it)->tryToBlock(); dp_block)
            return dp_block->getSchema();
    }
    return {};
}

void DeltaValueSpace::checkPacks(const DeltaPacks & new_packs)
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

        new_rows += pack->getRows();
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

DeltaValueSpace::DeltaValueSpace(PageId id_, const DeltaPacks & packs_)
    : id(id_), packs(packs_), delta_index(std::make_shared<DeltaIndex>()), log(&Logger::get("DeltaValueSpace"))
{
    for (auto & pack : packs)
    {
        rows += pack->getRows();
        bytes += pack->getBytes();
        deletes += pack->isDeleteRange();
        if (!pack->isSaved())
        {
            unsaved_rows += pack->getRows();
            unsaved_bytes += pack->getBytes();
            unsaved_deletes += pack->isDeleteRange();
        }
    }
}

void DeltaValueSpace::abandon(DMContext & context)
{
    bool v = false;
    if (!abandoned.compare_exchange_strong(v, true))
        throw Exception("Try to abandon a already abandoned DeltaValueSpace", ErrorCodes::LOGICAL_ERROR);

    if (auto manager = context.db_context.getDeltaIndexManager(); manager)
        manager->deleteRef(delta_index);
}

DeltaValueSpacePtr DeltaValueSpace::restore(DMContext & context, const RowKeyRange & segment_range, PageId id)
{
    Page                 page = context.storage_pool.meta().read(id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    auto                 packs = deserializePacks(context, segment_range, buf);
    return std::make_shared<DeltaValueSpace>(id, packs);
}

void DeltaValueSpace::saveMeta(WriteBatches & wbs) const
{
    MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
    // Only serialize saved packs.
    serializeSavedPacks(buf, packs);
    auto data_size = buf.count();
    wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

DeltaPacks DeltaValueSpace::checkHeadAndCloneTail(DMContext &         context,
                                                  const RowKeyRange & target_range,
                                                  const DeltaPacks &  head_packs,
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
        if ((*it_1)->getId() != (*it_2)->getId() || (*it_1)->getRows() != (*it_2)->getRows())
        {
            LOG_ERROR(log,
                      simpleInfo() << ", Delta  Check head packs failed, unexpected size. head_packs: " << packsToString(head_packs)
                                   << ", packs: " << packsToString(packs));
            throw Exception("Check head packs failed", ErrorCodes::LOGICAL_ERROR);
        }
    }

    DeltaPacks cloned_tail;
    for (; it_2 != packs.end(); ++it_2)
    {
        auto & pack = *it_2;
        if (auto dr = pack->tryToDeleteRange(); dr)
        {
            auto new_dr = dr->getDeleteRange().shrink(target_range);
            if (!new_dr.none())
            {
                // Only use the available delete_range pack.
                cloned_tail.push_back(dr->cloneWith(new_dr));
            }
        }
        else if (auto b = pack->tryToBlock(); b)
        {
            PageId new_data_page_id = 0;
            if (b->getDataPageId())
            {
                // Use a newly created page_id to reference the data page_id of current pack.
                new_data_page_id = context.storage_pool.newLogPageId();
                wbs.log.putRefPage(new_data_page_id, b->getDataPageId());
            }

            auto new_pack = b->cloneWith(new_data_page_id);

            // No matter or what, don't append to packs which cloned from old packs again.
            // Because they could shared the same cache. And the cache can NOT be inserted from different packs in different delta.
            new_pack->disableAppend();

            cloned_tail.push_back(new_pack);
        }
        else if (auto f = pack->tryToFile(); f)
        {
            auto delegator = context.path_pool.getStableDiskDelegator();
            auto new_ref_id = context.storage_pool.newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
            auto file_id = f->getFile()->fileId();
            wbs.data.putRefPage(new_ref_id, file_id);
            auto file_parent_path = delegator.getDTFilePath(file_id);
            auto new_file = DMFile::restore(context.db_context.getFileProvider(), file_id, /* ref_id= */ new_ref_id, file_parent_path);

            auto new_pack = f->cloneWith(context, new_file, target_range);
            cloned_tail.push_back(new_pack);
        }
    }

    return cloned_tail;
}

size_t DeltaValueSpace::getTotalCacheRows() const
{
    std::scoped_lock lock(mutex);
    size_t           cache_rows = 0;
    for (auto & pack : packs)
    {
        if (auto p = pack->tryToBlock(); p)
        {
            if (auto && c = p->getCache(); c)
                cache_rows += c->block.rows();
        }
    }
    return cache_rows;
}

size_t DeltaValueSpace::getTotalCacheBytes() const
{
    std::scoped_lock lock(mutex);
    size_t           cache_bytes = 0;
    for (auto & pack : packs)
    {
        if (auto p = pack->tryToBlock(); p)
        {
            if (auto && c = p->getCache(); c)
                cache_bytes += c->block.allocatedBytes();
        }
    }
    return cache_bytes;
}

size_t DeltaValueSpace::getValidCacheRows() const
{
    std::scoped_lock lock(mutex);
    size_t           cache_rows = 0;
    for (auto & pack : packs)
    {
        if (auto p = pack->tryToBlock(); p)
        {
            if (p->isCached())
                cache_rows += pack->getRows();
        }
    }
    return cache_rows;
}

void DeltaValueSpace::recordRemovePacksPages(WriteBatches & wbs) const
{
    for (auto & pack : packs)
        pack->removeData(wbs);
}

void DeltaValueSpace::appendPackInner(const DeltaPackPtr & pack)
{
    auto last_schema = lastSchema();

    if (auto dp_block = pack->tryToBlock(); dp_block)
    {
        // If this pack's schema is identical to last_schema, then use the last_schema instance,
        // so that we don't have to serialize my_schema instance.
        auto my_schema = dp_block->getSchema();
        if (last_schema && my_schema && last_schema != my_schema && isSameSchema(*my_schema, *last_schema))
            dp_block->resetIdenticalSchema(last_schema);
    }

    if (!packs.empty())
    {
        auto last_pack = packs.back();
        if (last_pack->isBlock())
            last_pack->tryToBlock()->disableAppend();
    }

    packs.push_back(pack);

    rows += pack->getRows();
    bytes += pack->getBytes();
    deletes += pack->getDeletes();

    unsaved_rows += pack->getRows();
    unsaved_bytes += pack->getBytes();
    unsaved_deletes += pack->getDeletes();
}

bool DeltaValueSpace::appendPack(DMContext & /*context*/, const DeltaPackPtr & pack)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    appendPackInner(pack);

    return true;
}

bool DeltaValueSpace::appendToCache(DMContext & context, const Block & block, size_t offset, size_t limit)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    // If last pack has a valid cache block, and it is mutable (haven't been saved to disk yet), we will merge the newly block into last pack.
    // Otherwise, create a new cache block and write into it.

    DeltaPackBlock * mutable_pack{};
    if (!packs.empty())
    {
        auto & last_pack = packs.back();
        if (last_pack->isBlock())
        {
            if (auto p = last_pack->tryToBlock(); p->isAppendable())
            {
                if constexpr (DM_RUN_CHECK)
                {
                    if (unlikely(!isSameSchema(*p->getSchema(), p->getCache()->block)))
                        throw Exception("Mutable pack's structure of schema and block are different: " + last_pack->toString());
                }

                auto & cache_block = p->getCache()->block;
                bool   is_overflow
                    = cache_block.rows() >= context.delta_cache_limit_rows || cache_block.bytes() >= context.delta_cache_limit_bytes;
                bool is_same_schema = isSameSchema(block, cache_block);
                if (!is_overflow && is_same_schema)
                {
                    // The last cache block is available
                    mutable_pack = p;
                }
            }
        }
    }

    size_t append_bytes = block.bytes(offset, limit);

    if (!mutable_pack)
    {
        // Create a new pack.
        auto last_schema = lastSchema();
        auto my_schema   = (last_schema && isSameSchema(block, *last_schema)) ? last_schema : std::make_shared<Block>(block.cloneEmpty());

        auto new_pack = DeltaPackBlock::createCachePack(my_schema);
        appendPackInner(new_pack);

        mutable_pack = new_pack->tryToBlock();
    }

    mutable_pack->appendToCache(block, offset, limit, append_bytes);

    rows += limit;
    bytes += append_bytes;
    unsaved_rows += limit;
    unsaved_bytes += append_bytes;

    return true;
}

bool DeltaValueSpace::appendDeleteRange(DMContext & /*context*/, const RowKeyRange & delete_range)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    auto p = std::make_shared<DeltaPackDeleteRange>(delete_range);
    appendPackInner(p);

    return true;
}

bool DeltaValueSpace::ingestPacks(DMContext & /*context*/, const RowKeyRange & range, const DeltaPacks & packs, bool clear_data_in_range)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    // Prepend a DeleteRange to clean data before applying packs
    if (clear_data_in_range)
    {
        auto p = std::make_shared<DeltaPackDeleteRange>(range);
        appendPackInner(p);
    }

    for (auto & p : packs)
    {
        appendPackInner(p);
    }

    return true;
}

} // namespace DM
} // namespace DB

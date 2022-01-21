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
    for (auto it = column_files.rbegin(); it != column_files.rend(); ++it)
    {
        if (auto * m_file = (*it)->tryToInMemoryFile(); m_file)
            return m_file->getSchema();
        else if (auto * t_file = (*it)->tryToTinyFile(); t_file)
            return t_file->getSchema();
    }
    return {};
}

void DeltaValueSpace::checkPacks(const ColumnFiles & new_column_files)
{
    if constexpr (!DM_RUN_CHECK)
        return;
    size_t new_rows = 0;
    size_t new_deletes = 0;

    bool seen_unsaved = false;
    bool ok = true;
    for (const auto & pack : new_column_files)
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
                  "Rows and deletes check failed. Current packs: " << columnFilesToString(column_files) << ", new packs: " << columnFilesToString(column_files));
        throw Exception("Rows and deletes check failed.", ErrorCodes::LOGICAL_ERROR);
    }
}

// ================================================
// Public methods
// ================================================

DeltaValueSpace::DeltaValueSpace(PageId id_, const ColumnFiles & column_files_)
    : id(id_)
    , column_files(column_files_)
    , delta_index(std::make_shared<DeltaIndex>())
    , log(&Poco::Logger::get("DeltaValueSpace"))
{
    for (auto & pack : column_files)
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
    Page page = context.storage_pool.meta()->read(id, nullptr);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    auto packs = deserializeColumnStableFiles(context, segment_range, buf);
    return std::make_shared<DeltaValueSpace>(id, packs);
}

void DeltaValueSpace::saveMeta(WriteBatches & wbs) const
{
    MemoryWriteBuffer buf(0, COLUMN_FILE_SERIALIZE_BUFFER_SIZE);
    // Only serialize saved packs.
    serializeColumnStableFiles(buf, column_files);
    auto data_size = buf.count();
    wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

ColumnFiles DeltaValueSpace::checkHeadAndCloneTail(DMContext & context,
                                                   const RowKeyRange & target_range,
                                                   const ColumnFiles & head_packs,
                                                   WriteBatches & wbs) const
{
    if (head_packs.size() > column_files.size())
    {
        LOG_ERROR(log,
                  info() << ", Delta  Check head packs failed, unexpected size. head_packs: " << columnFilesToString(head_packs)
                         << ", packs: " << columnFilesToString(column_files));
        throw Exception("Check head packs failed, unexpected size", ErrorCodes::LOGICAL_ERROR);
    }

    auto it_1 = head_packs.begin();
    auto it_2 = column_files.begin();
    for (; it_1 != head_packs.end() && it_2 != column_files.end(); ++it_1, ++it_2)
    {
        if ((*it_1)->getId() != (*it_2)->getId() || (*it_1)->getRows() != (*it_2)->getRows())
        {
            LOG_ERROR(log,
                      simpleInfo() << ", Delta  Check head packs failed, unexpected size. head_packs: " << columnFilesToString(head_packs)
                                   << ", packs: " << columnFilesToString(column_files));
            throw Exception("Check head packs failed", ErrorCodes::LOGICAL_ERROR);
        }
    }

    ColumnFiles cloned_tail;
    for (; it_2 != column_files.end(); ++it_2)
    {
        const auto & pack = *it_2;
        if (auto * dr = pack->tryToDeleteRange(); dr)
        {
            auto new_dr = dr->getDeleteRange().shrink(target_range);
            if (!new_dr.none())
            {
                // Only use the available delete_range pack.
                cloned_tail.push_back(dr->cloneWith(new_dr));
            }
        }
        else if (auto * b = pack->tryToInMemoryFile(); b)
        {
            auto new_pack = b->clone();

            // No matter or what, don't append to packs which cloned from old packs again.
            // Because they could shared the same cache. And the cache can NOT be inserted from different packs in different delta.
            new_pack->disableAppend();

            cloned_tail.push_back(new_pack);
        }
        else if (auto * t = pack->tryToTinyFile(); t)
        {
            // Use a newly created page_id to reference the data page_id of current pack.
            PageId new_data_page_id = context.storage_pool.newLogPageId();
            wbs.log.putRefPage(new_data_page_id, t->getDataPageId());
            auto new_pack = t->cloneWith(new_data_page_id);

            cloned_tail.push_back(new_pack);
        }
        else if (auto f = pack->tryToBigFile(); f)
        {
            auto delegator = context.path_pool.getStableDiskDelegator();
            auto new_ref_id = context.storage_pool.newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
            auto file_id = f->getFile()->fileId();
            wbs.data.putRefPage(new_ref_id, file_id);
            auto file_parent_path = delegator.getDTFilePath(file_id);
            auto new_file = DMFile::restore(context.db_context.getFileProvider(), file_id, /* ref_id= */ new_ref_id, file_parent_path, DMFile::ReadMetaMode::all());

            auto new_pack = f->cloneWith(context, new_file, target_range);
            cloned_tail.push_back(new_pack);
        }
    }

    return cloned_tail;
}

size_t DeltaValueSpace::getTotalCacheRows() const
{
    std::scoped_lock lock(mutex);
    size_t cache_rows = 0;
    for (const auto & pack : column_files)
    {
        if (auto * p = pack->tryToInMemoryFile(); p)
        {
            if (auto && c = p->getCache(); c)
                cache_rows += c->block.rows();
        }
        else if (auto * t = pack->tryToTinyFile(); t)
        {
            if (auto && c = t->getCache(); c)
                cache_rows += c->block.rows();
        }
    }
    return cache_rows;
}

size_t DeltaValueSpace::getTotalCacheBytes() const
{
    std::scoped_lock lock(mutex);
    size_t cache_bytes = 0;
    for (auto & pack : column_files)
    {
        if (auto p = pack->tryToInMemoryFile(); p)
        {
            if (auto && c = p->getCache(); c)
                cache_bytes += c->block.allocatedBytes();
        }
        else if (auto * t = pack->tryToTinyFile(); t)
        {
            if (auto && c = t->getCache(); c)
                cache_bytes += c->block.allocatedBytes();
        }
    }
    return cache_bytes;
}

size_t DeltaValueSpace::getValidCacheRows() const
{
    std::scoped_lock lock(mutex);
    size_t cache_rows = 0;
    for (auto & pack : column_files)
    {
        if (auto p = pack->tryToInMemoryFile(); p)
        {
            cache_rows += pack->getRows();
        }
        else if (auto * t = pack->tryToTinyFile(); t)
        {
            if (auto && c = t->getCache(); c)
                cache_rows += c->block.rows();
        }
    }
    return cache_rows;
}

void DeltaValueSpace::recordRemovePacksPages(WriteBatches & wbs) const
{
    for (auto & pack : column_files)
        pack->removeData(wbs);
}

void DeltaValueSpace::appendPackInner(const ColumnFilePtr & pack)
{
    auto last_schema = lastSchema();

    if (auto * m_file = pack->tryToInMemoryFile(); m_file)
    {
        // If this pack's schema is identical to last_schema, then use the last_schema instance,
        // so that we don't have to serialize my_schema instance.
        auto my_schema = m_file->getSchema();
        if (last_schema && my_schema && last_schema != my_schema && isSameSchema(*my_schema, *last_schema))
            m_file->resetIdenticalSchema(last_schema);
    }
    else if (auto * t_file = pack->tryToTinyFile(); t_file)
    {
        // If this pack's schema is identical to last_schema, then use the last_schema instance,
        // so that we don't have to serialize my_schema instance.
        auto my_schema = t_file->getSchema();
        if (last_schema && my_schema && last_schema != my_schema && isSameSchema(*my_schema, *last_schema))
            t_file->resetIdenticalSchema(last_schema);
    }

    if (!column_files.empty())
    {
        auto last_pack = column_files.back();
        if (last_pack->isAppendable())
            last_pack->disableAppend();
    }

    column_files.push_back(pack);

    rows += pack->getRows();
    bytes += pack->getBytes();
    deletes += pack->getDeletes();

    unsaved_rows += pack->getRows();
    unsaved_bytes += pack->getBytes();
    unsaved_deletes += pack->getDeletes();
}

bool DeltaValueSpace::appendPack(DMContext & /*context*/, const ColumnFilePtr & pack)
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

    // If the `column_files` is not empty, and the last `column_file` is a `ColumnInMemoryFile`, we will merge the newly block into the last `column_file`.
    // Otherwise, create a new `ColumnInMemoryFile` and write into it.
    bool success = false;
    size_t append_bytes = block.bytes(offset, limit);
    if (!column_files.empty())
    {
        auto & last_column_file = column_files.back();
        if (last_column_file->isAppendable())
            success = last_column_file->append(context, block, offset, limit, append_bytes);
    }

    if (!success)
    {
        // Create a new pack.
        auto last_schema = lastSchema();
        auto my_schema = (last_schema && isSameSchema(block, *last_schema)) ? last_schema : std::make_shared<Block>(block.cloneEmpty());
        auto new_column_file = std::make_shared<ColumnInMemoryFile>(my_schema);
        appendPackInner(new_column_file);
        success = new_column_file->append(context, block, offset, limit, append_bytes);
        if (unlikely(!success))
            throw Exception("Write to MemTableSet failed", ErrorCodes::LOGICAL_ERROR);
    }

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

    auto p = std::make_shared<ColumnDeleteRangeFile>(delete_range);
    appendPackInner(p);

    return true;
}

bool DeltaValueSpace::ingestColumnFiles(DMContext & /*context*/, const RowKeyRange & range, const ColumnFiles & packs, bool clear_data_in_range)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    // Prepend a DeleteRange to clean data before applying packs
    if (clear_data_in_range)
    {
        auto p = std::make_shared<ColumnDeleteRangeFile>(range);
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

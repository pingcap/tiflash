#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DiskValueSpace.h>
#include <Storages/DeltaMerge/ReorganizeBlockInputStream.h>
#include <Storages/Page/PageStorage.h>

#include <IO/WriteHelpers.h>
#include <ext/scope_guard.h>

namespace ProfileEvents
{
extern const Event DMFlushDeltaCache;
extern const Event DMFlushDeltaCacheNS;
} // namespace ProfileEvents

namespace DB
{
namespace DM
{
//==========================================================================================
// DiskValueSpace public methods.
//==========================================================================================

DiskValueSpace::DiskValueSpace(bool is_delta_vs_, PageId page_id_, Packs && packs_, MutableColumnMap && cache_, size_t cache_packs_)
    : is_delta_vs(is_delta_vs_),
      page_id(page_id_),
      packs(std::move(packs_)),
      cache(std::move(cache_)),
      cache_packs(cache_packs_),
      log(&Logger::get("DiskValueSpace"))
{
}

DiskValueSpace::DiskValueSpace(const DiskValueSpace & other)
    : is_delta_vs(other.is_delta_vs), //
      page_id(other.page_id),
      packs(other.packs),
      cache_packs(other.cache_packs),
      log(other.log)
{
    for (auto & [col_id, col] : other.cache)
        cache.emplace(col_id, col->cloneResized(col->size()));
}

void DiskValueSpace::restore(const OpContext & context)
{
    // Deserialize.
    Page                 page = context.meta_storage.read(page_id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    packs = deserializePacks(buf);

    // Restore cache.
    if (is_delta_vs && !packs.empty())
    {
        size_t packs_to_cache = 0;
        for (ssize_t i = packs.size() - 1; i >= 0; --i)
        {
            const auto & pack = packs[i];
            // We can not cache a pack which is a delete range and not the latest one.
            if (pack.isDeleteRange() && i != (ssize_t)packs.size() - 1)
                break;
            if (pack.getRows() >= context.dm_context.delta_cache_limit_rows)
                break;
            ++packs_to_cache;
        }

        // Load cache into memory.
        size_t     total_rows = num_rows();
        size_t     cache_rows = rowsFromBack(packs_to_cache);
        PageReader page_reader(context.data_storage);
        Block      cache_data = read(context.dm_context.store_columns, page_reader, total_rows - cache_rows, cache_rows);
        if (unlikely(cache_data.rows() != cache_rows))
            throw Exception("The fragment rows from storage mismatch");

        for (const auto & col_define : context.dm_context.store_columns)
        {
            ColumnWithTypeAndName & col = cache_data.getByName(col_define.name);
            cache[col_define.id]        = (*std::move(col.column)).mutate();
        }
        // Note: cache_packs must be set after #read() .
        cache_packs = packs_to_cache;
    }

    // Try flush now, in case of last flush failure recover.
    WriteBatch remove_data_wb;
    auto       new_instance = tryFlushCache(context, remove_data_wb);
    if (new_instance)
    {
        packs.swap(new_instance->packs);
        cache.swap(new_instance->cache);
        cache_packs = new_instance->cache_packs;
    }
    context.data_storage.write(std::move(remove_data_wb));
}


AppendTaskPtr DiskValueSpace::createAppendTask(const OpContext & context, WriteBatches & wbs, const BlockOrDelete & update) const
{
    assert(is_delta_vs == true);

    auto task = std::make_unique<AppendTask>();

    auto & append_block = update.block;
    auto & delete_range = update.delete_range;

    const bool   is_delete       = !append_block;
    const size_t append_rows     = is_delete ? 0 : append_block.rows();
    const size_t cache_rows      = cacheRows();
    const size_t total_rows      = num_rows();
    const size_t in_storage_rows = total_rows - cache_rows;

    // If the newly appended object is a delete_range, then we must clear the cache and flush them.

    if (is_delta_vs && !is_delete //
        && (cache_rows + append_rows) < context.dm_context.delta_cache_limit_rows)
    {
        // Simply put the newly appended block into cache.
        Pack pack = preparePackDataWrite(context.dm_context, context.gen_data_page_id, wbs.log, append_block);

        task->append_cache       = true;
        task->remove_packs_back = 0;

        task->append_packs.emplace_back(std::move(pack));
    }
    else
    {
        // Flush cache.

        task->append_cache       = false;
        task->remove_packs_back = cache_packs;

        if (!cache_packs)
        {
            // There are no caches, simple write the newly block or delete_range is enough.
            Pack pack
                = is_delete ? Pack(delete_range) : preparePackDataWrite(context.dm_context, context.gen_data_page_id, wbs.log, append_block);

            task->append_packs.emplace_back(std::move(pack));
        }
        else
        {
            // Merge the former caches packs and the newly block into one big pack, and then flush it.
            // delete_range is stored by a separated pack.

            size_t cache_start = packs.size() - cache_packs;
            for (size_t i = cache_start; i < packs.size(); ++i)
            {
                auto & old_pack = packs[i];
                for (const auto & [col_id, col_meta] : old_pack.getMetas())
                {
                    (void)col_id;
                    wbs.removed_log.delPage(col_meta.page_id);
                }
            }

            Block  compacted_block;
            size_t compacted_rows = cache_rows + append_rows;
            if (cache.empty())
            {
                // TODO: Currently in write thread, we do not use snapshot read. This may need to refactor later.

                PageReader page_reader(context.data_storage);
                // Load fragment packs' data from disk.
                compacted_block = read(context.dm_context.store_columns, //
                                       page_reader,
                                       in_storage_rows,
                                       cache_rows,
                                       compacted_rows);

                if (unlikely(compacted_block.rows() != cache_rows))
                    throw Exception("The fragment rows from storage mismatch");

                if (!is_delete)
                    concat(compacted_block, append_block);
            }
            else
            {
                // Use the cache.
                for (const auto & col_define : context.dm_context.store_columns)
                {
                    auto new_col = col_define.type->createColumn();
                    new_col->reserve(compacted_rows);
                    new_col->insertRangeFrom(*cache.at(col_define.id), 0, cache_rows);
                    if (!is_delete)
                        new_col->insertRangeFrom(*append_block.getByName(col_define.name).column, 0, append_rows);

                    ColumnWithTypeAndName col(std::move(new_col), col_define.type, col_define.name, col_define.id);
                    compacted_block.insert(std::move(col));
                }
            }

            Pack compacted_pack = preparePackDataWrite(context.dm_context, context.gen_data_page_id, wbs.log, compacted_block);

            task->append_packs.emplace_back(std::move(compacted_pack));
            if (is_delete)
                task->append_packs.emplace_back(Pack(delete_range));
        }
    }

    return task;
}

void DiskValueSpace::applyAppendToWriteBatches(const AppendTaskPtr & task, WriteBatches & wbs)
{
    assert(is_delta_vs == true);
    // Serialize meta updates and return by wbs.meta, later we will commit them in DeltaMergeStore level.
    MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
    serializePacks(buf, packs.begin(), packs.begin() + (packs.size() - task->remove_packs_back), task->append_packs);
    const auto data_size = buf.count();
    wbs.meta.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);
}

DiskValueSpacePtr DiskValueSpace::applyAppendTask(const OpContext & context, const AppendTaskPtr & task, const BlockOrDelete & update)
{
    assert(is_delta_vs == true);

    // Apply in memory.
    DiskValueSpace * instance = this;
    if (task->remove_packs_back > 0)
    {
        // If any packs got removed, then we create a new instance.
        // Make sure a reference to an instance is read constantly by rows.
        Packs new_packs(packs.begin(), packs.end() - task->remove_packs_back);
        instance = new DiskValueSpace(is_delta_vs, page_id, std::move(new_packs));
    }

    for (auto & pack : task->append_packs)
        instance->packs.emplace_back(std::move(pack));

    if (task->append_cache)
    {
        if (unlikely(instance != this))
            throw Exception("cache should only be applied to this");

        auto block_rows = update.block.rows();
        for (const auto & col_define : context.dm_context.store_columns)
        {
            const ColumnWithTypeAndName & col = update.block.getByName(col_define.name);

            auto it = instance->cache.find(col_define.id);
            if (it == instance->cache.end())
                instance->cache.emplace(col_define.id, col_define.type->createColumn());

            instance->cache[col_define.id]->insertRangeFrom(*col.column, 0, block_rows);
        }
        ++instance->cache_packs;
    }
    else
    {
        instance->cache.clear();
        instance->cache_packs = 0;
    }

    if (instance != this)
        return DiskValueSpacePtr(instance);
    else
        return {};
}

Packs DiskValueSpace::writePacks(const OpContext & context, const BlockInputStreamPtr & sorted_input_stream, WriteBatch & wb)
{
    // Cleanning up the boundary between blocks.
    const String &             pk_column_name = context.dm_context.handle_column.name;
    ReorganizeBlockInputStream stream(sorted_input_stream, pk_column_name);

    Packs packs;
    while (true)
    {
        Block block = stream.read();
        if (!block)
            break;
        Pack pack = preparePackDataWrite(context.dm_context, context.gen_data_page_id, wb, block);
        packs.push_back(std::move(pack));
    }

    if constexpr (DM_RUN_CHECK)
    {
        //  Sanity check the boundary between different packs is not overlap
        if (packs.size() > 1)
        {
            for (size_t i = 1; i < packs.size(); ++i)
            {
                const Pack & prev = packs[i - 1];
                const Pack & curr = packs[i];
                if (prev.isDeleteRange() || curr.isDeleteRange())
                {
                    throw Exception("Unexpected DeleteRange in stable inputstream. prev:" + prev.info() + " curr: " + curr.info(),
                                    ErrorCodes::LOGICAL_ERROR);
                }

                const HandlePair prev_handle = prev.getHandleFirstLast();
                const HandlePair curr_handle = curr.getHandleFirstLast();
                // pk should be increasing and no overlap between packs
                if (prev_handle.second >= curr_handle.first)
                {
                    throw Exception("Overlap packs between " + prev.info() + " and " + curr.info(), ErrorCodes::LOGICAL_ERROR);
                }
            }
        }
    }
    return packs;
}

Pack DiskValueSpace::writeDelete(const OpContext &, const HandleRange & delete_range)
{
    return Pack(delete_range);
}

void DiskValueSpace::clearPacks(WriteBatch & removed_wb)
{
    for (const auto & c : packs)
    {
        for (const auto & m : c.getMetas())
            removed_wb.delPage(m.second.page_id);
    }

    Packs tmp;
    packs.swap(tmp);

    cache.clear();
    cache_packs = 0;
}

void DiskValueSpace::replacePacks(
    WriteBatch & meta_wb, WriteBatch & removed_wb, Packs && new_packs, MutableColumnMap && cache_, size_t cache_packs_)
{
    MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
    serializePacks(buf, new_packs.begin(), new_packs.end(), {});
    auto data_size = buf.count();
    meta_wb.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);

    for (const auto & c : packs)
    {
        for (const auto & m : c.getMetas())
            removed_wb.delPage(m.second.page_id);
    }

    packs.swap(new_packs);

    cache        = std::move(cache_);
    cache_packs = cache_packs_;
}

void DiskValueSpace::replacePacks(WriteBatch & meta_wb, WriteBatch & removed_wb, Packs && new_packs)
{
    replacePacks(meta_wb, removed_wb, std::move(new_packs), {}, 0);
}

void DiskValueSpace::setPacks(WriteBatch & meta_wb, Packs && new_packs)
{
    setPacksAndCache(meta_wb, std::move(new_packs), {}, 0);
}

void DiskValueSpace::setPacksAndCache(WriteBatch & meta_wb, Packs && new_packs, MutableColumnMap && cache_, size_t cache_packs_)
{
    MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
    serializePacks(buf, new_packs.begin(), new_packs.end(), {});
    auto data_size = buf.count();
    meta_wb.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);

    cache        = std::move(cache_);
    cache_packs = cache_packs_;
}

void DiskValueSpace::appendPackWithCache(const OpContext & context, Pack && pack, const Block & block)
{
    // should only append to delta
    assert(is_delta_vs);
    {
        MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
        serializePacks(buf, packs.begin(), packs.end(), &pack);
        auto       data_size = buf.count();
        WriteBatch meta_wb;
        meta_wb.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);
        context.meta_storage.write(std::move(meta_wb));
    }

    packs.push_back(std::move(pack));

    auto write_rows = block.rows();
    if (!write_rows)
    {
        // If it is an empty pack, then nothing to append.
        ++cache_packs;
        return;
    }

    // If former cache is empty, and this pack is big enough, then no need to cache.
    if (cache_packs == 0 && write_rows >= context.dm_context.delta_cache_limit_rows)
        return;

    for (const auto & col_define : context.dm_context.store_columns)
    {
        const ColumnWithTypeAndName & col = block.getByName(col_define.name);

        auto it = cache.find(col_define.id);
        if (it == cache.end())
            cache.emplace(col_define.id, col_define.type->createColumn());

        cache[col_define.id]->insertRangeFrom(*col.column, 0, write_rows);
    }
    ++cache_packs;
}

Block DiskValueSpace::read(const ColumnDefines & read_column_defines,
                           const PageReader &    page_reader,
                           size_t                rows_offset,
                           size_t                rows_limit,
                           std::optional<size_t> reserve_rows_) const
{
    if (read_column_defines.empty())
        return {};
    rows_limit = std::min(rows_limit, num_rows() - rows_offset);

    size_t         reserve_rows = reserve_rows_ ? *reserve_rows_ : rows_limit;
    MutableColumns columns;
    for (const auto & define : read_column_defines)
    {
        columns.emplace_back(define.type->createColumn());
        columns.back()->reserve(reserve_rows);
    }

    auto [start_pack_index, rows_start_in_start_pack] = findPack(rows_offset);
    auto [end_pack_index, rows_end_in_end_pack]       = findPack(rows_offset + rows_limit);

    size_t pack_cache_start = packs.size() - cache_packs;
    size_t already_read_rows = 0;

    /// For the packs in storage, we read pack by pack, instead of column by column.
    /// Because the column data of a same pack are likely to be stored together, or at least closed.
    /// While for the packs in cache, we read column by column.

    size_t pack_index = start_pack_index;
    for (; pack_index < pack_cache_start && pack_index <= end_pack_index; ++pack_index)
    {
        const auto & cur_pack = packs[pack_index];

        if (cur_pack.getRows())
        {
            size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
            size_t rows_end_in_pack   = pack_index == end_pack_index ? rows_end_in_end_pack : cur_pack.getRows();

            if (rows_end_in_pack > rows_start_in_pack)
            {
                readPackData(
                    columns, read_column_defines, cur_pack, page_reader, rows_start_in_pack, rows_end_in_pack - rows_start_in_pack);

                already_read_rows += rows_end_in_pack - rows_start_in_pack;
            }
        }
    }

    /// The reset of data is in cache, simply append them into result.

    if (already_read_rows < rows_limit)
    {
        // TODO We do flush each time in `StorageDeltaMerge::alterImpl`, so that there is only the data with newest schema in cache. We ignore either new inserted col nor col type changed in cache for now.

        // pack_index could be larger than pack_cache_start.
        size_t cache_rows_offset = 0;
        for (size_t i = pack_cache_start; i < pack_index; ++i)
            cache_rows_offset += packs[i].getRows();

        for (size_t index = 0; index < read_column_defines.size(); ++index)
        {
            const ColumnDefine & define    = read_column_defines[index];
            auto &               cache_col = cache.at(define.id); // TODO new inserted col'id don't exist in cache.

            size_t rows_offset_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;

            // TODO columns[index].type maybe not consisted with cache_col after ddl.
            columns[index]->insertRangeFrom(*cache_col, cache_rows_offset + rows_offset_in_pack, rows_limit - already_read_rows);
        }
    }

    Block res;
    for (size_t index = 0; index < read_column_defines.size(); ++index)
    {
        const ColumnDefine &  define = read_column_defines[index];
        ColumnWithTypeAndName col(std::move(columns[index]), define.type, define.name, define.id);
        res.insert(std::move(col));
    }
    return res;
}

Block DiskValueSpace::read(const ColumnDefines & read_column_defines, const PageReader & page_reader, size_t pack_index) const
{
    if (read_column_defines.empty())
        return {};

    const auto & pack = packs.at(pack_index);

    MutableColumns columns;
    for (const auto & define : read_column_defines)
    {
        columns.emplace_back(define.type->createColumn());
        columns.back()->reserve(pack.getRows());
    }

    if (pack.getRows())
    {
        size_t pack_cache_start = packs.size() - cache_packs;
        if (pack_index < pack_cache_start)
        {
            // Read from storage
            readPackData(columns, read_column_defines, pack, page_reader, 0, pack.getRows());
        }
        else
        {
            // Read from cache

            // TODO We do flush each time in `StorageDeltaMerge::alterImpl`, so that there is only the data with newest schema in cache. We ignore either new inserted col nor col type changed in cache for now.
            size_t cache_rows_offset = 0;
            for (size_t i = pack_cache_start; i < pack_index; ++i)
                cache_rows_offset += packs[i].getRows();

            for (size_t index = 0; index < read_column_defines.size(); ++index)
            {
                const auto & define    = read_column_defines[index];
                auto &       cache_col = cache.at(define.id);
                columns[index]->insertRangeFrom(*cache_col, cache_rows_offset, pack.getRows());
            }
        }
    }

    Block res;
    for (size_t index = 0; index < read_column_defines.size(); ++index)
    {
        const ColumnDefine &  define = read_column_defines[index];
        ColumnWithTypeAndName col(std::move(columns[index]), define.type, define.name, define.id);
        res.insert(std::move(col));
    }
    return res;
}

BlockOrDeletes DiskValueSpace::getMergeBlocks(const ColumnDefine & handle,
                                              const PageReader &   page_reader,
                                              size_t               rows_begin,
                                              size_t               deletes_begin,
                                              size_t               rows_end,
                                              size_t               deletes_end) const
{
    BlockOrDeletes res;

    auto [start_pack_index, rows_start_in_start_pack] = findPack(rows_begin, deletes_begin);
    auto [end_pack_index, rows_end_in_end_pack]       = findPack(rows_end, deletes_end);

    size_t block_rows_start = rows_begin;
    size_t block_rows_end   = rows_begin;

    for (size_t pack_index = start_pack_index; pack_index < packs.size() && pack_index <= end_pack_index; ++pack_index)
    {
        const Pack & pack = packs[pack_index];

        size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
        size_t rows_end_in_pack   = pack_index == end_pack_index ? rows_end_in_end_pack : pack.getRows();

        block_rows_end += rows_end_in_pack - rows_start_in_pack;

        if (pack.isDeleteRange() || (pack_index == packs.size() - 1 || pack_index == end_pack_index))
        {
            if (block_rows_end != block_rows_start)
                res.emplace_back(
                    read({handle, getVersionColumnDefine()}, page_reader, block_rows_start, block_rows_end - block_rows_start));
            if (pack.isDeleteRange())
                res.emplace_back(pack.getDeleteRange());

            block_rows_start = block_rows_end;
        }
    }

    return res;
}

DiskValueSpacePtr DiskValueSpace::tryFlushCache(const OpContext & context, WriteBatch & remove_data_wb, bool force)
{
    if (cache_packs == 0)
        return {};

    // If last pack is a delete range, we should flush cache.
    HandleRange delete_range = packs.back().isDeleteRange() ? packs.back().getDeleteRange() : HandleRange::newNone();
    if (!delete_range.none())
        force = true;

    const size_t cache_rows = cacheRows();
    if (!force && cache_rows < context.dm_context.delta_cache_limit_rows)
        return {};

    return doFlushCache(context, remove_data_wb);
}

DiskValueSpacePtr DiskValueSpace::doFlushCache(const OpContext & context, WriteBatch & remove_data_wb)
{
    const size_t cache_rows      = cacheRows();
    const size_t total_rows      = num_rows();
    const size_t in_storage_rows = total_rows - cache_rows;

    HandleRange delete_range = packs.back().isDeleteRange() ? packs.back().getDeleteRange() : HandleRange::newNone();

    if (cache_packs <= 1)
    {
        // One pack no need to compact.
        cache.clear();
        cache_packs = 0;
        return {};
    }

    // Create an new instance.
    Packs tmp_packs(packs.begin(), packs.end());
    auto   new_instance = std::make_shared<DiskValueSpace>(is_delta_vs, page_id, std::move(tmp_packs));

    EventRecorder recorder(ProfileEvents::DMFlushDeltaCache, ProfileEvents::DMFlushDeltaCacheNS);

    LOG_DEBUG(log, "Start flush cache");

    /// Flush cache to disk and replace the fragment packs.

    WriteBatch data_wb_insert;
    WriteBatch meta_wb;

    size_t cache_start = packs.size() - cache_packs;
    for (size_t i = cache_start; i < packs.size(); ++i)
    {
        auto & old_pack = packs[i];
        for (const auto & [col_id, col_meta] : old_pack.getMetas())
        {
            (void)col_id;
            remove_data_wb.delPage(col_meta.page_id);
        }
    }

    Block compacted;
    if (cache.empty())
    {
        // Load fragment data from disk.
        PageReader page_reader(context.data_storage);
        compacted = read(context.dm_context.store_columns, page_reader, in_storage_rows, cache_rows);

        if (unlikely(compacted.rows() != cache_rows))
            throw Exception("The fragment rows from storage mismatch");
    }
    else
    {
        // Use the cache.
        for (const auto & col_define : context.dm_context.store_columns)
        {
            ColumnWithTypeAndName col(cache.at(col_define.id)->cloneResized(cache_rows), col_define.type, col_define.name, col_define.id);
            compacted.insert(col);

            if (unlikely(col.column->size() != cache_rows))
                throw Exception("The cache rows mismatch");
        }
    }

    Pack compacted_pack = preparePackDataWrite(context.dm_context, context.gen_data_page_id, data_wb_insert, compacted);

    Packs new_packs(packs.begin(), packs.begin() + (packs.size() - cache_packs));
    new_packs.push_back(std::move(compacted_pack));
    if (!delete_range.none())
        new_packs.emplace_back(delete_range);

    {
        MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
        serializePacks(buf, new_packs.begin(), new_packs.end(), {});
        auto data_size = buf.count();
        meta_wb.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);
    }

    /// Note that: We have the risk of dirty page here. Because writing between different storage is not atomic.
    /// But the correctness is still guaranteed.

    // The order here is critical.
    context.data_storage.write(std::move(data_wb_insert));
    context.meta_storage.write(std::move(meta_wb));

    // ============================================================
    // The following code are pure memory operations,
    // they are considered safe and won't fail.

    new_instance->packs.swap(new_packs);
    new_instance->cache.clear();
    new_instance->cache_packs = 0;

    // ============================================================

    recorder.submit();

    LOG_DEBUG(log, "Done flush cache");

    return new_instance;
}

PackBlockInputStreamPtr DiskValueSpace::getInputStream(const ColumnDefines & read_columns, const PageReader & page_reader) const
{
    return std::make_shared<PackBlockInputStream>(packs, read_columns, page_reader, RSOperatorPtr());
}

DeltaValueSpacePtr DiskValueSpace::getValueSpace(const PageReader &    page_reader,
                                                 const ColumnDefines & read_columns,
                                                 const HandleRange &   range,
                                                 size_t                rows_limit) const
{
    auto mvs = std::make_shared<DeltaValueSpace>();

    size_t pack_cache_start = packs.size() - cache_packs;
    size_t already_read_rows = 0;
    size_t pack_index       = 0;
    for (; pack_index < pack_cache_start && pack_index < packs.size(); ++pack_index)
    {
        auto & pack = packs[pack_index];
        if (pack.isDeleteRange() || !pack.getRows())
            continue;
#if 0
        // FIXME: Disable filter since we need to use all values to build DeltaTree.
        auto & handle_meta            = pack.getColumn(EXTRA_HANDLE_COLUMN_ID);
        auto [min_handle, max_handle] = handle_meta.minmax->getIntMinMax(0);
        if (range.intersect(min_handle, max_handle))
            mvs->addBlock(read(read_columns, page_reader, pack_index), pack.getRows());
        else
            mvs->addBlock({}, pack.getRows());
#else
        (void)range;
        mvs->addBlock(read(read_columns, page_reader, pack_index), pack.getRows());
#endif

        already_read_rows += pack.getRows();
        if (already_read_rows >= rows_limit)
            break;
    }

    /// The reset of data is in cache, simply append them into result.

    if (already_read_rows < rows_limit)
    {
        Block block;
        auto  cache_limit = rows_limit - already_read_rows;
        for (size_t index = 0; index < read_columns.size(); ++index)
        {
            const ColumnDefine & define    = read_columns[index];
            auto &               cache_col = cache.at(define.id); // TODO new inserted col'id don't exist in cache.
            auto                 clone     = cache_col->cloneResized(cache_limit);
            block.insert(ColumnWithTypeAndName(std::move(clone), define.type, define.name, define.id));
        }
        mvs->addBlock(block, cache_limit);
    }

    return mvs;
}

Packs DiskValueSpace::getPacksBefore(size_t rows, size_t deletes) const
{
    auto [pack_index, offset_in_pack] = findPack(rows, deletes);
    if (offset_in_pack != 0)
        throw Exception("offset_in_pack is expected to be zero");
    return Packs(packs.begin(), packs.begin() + pack_index);
}

Packs DiskValueSpace::getPacksAfter(size_t rows, size_t deletes) const
{
    auto [pack_index, offset_in_pack] = findPack(rows, deletes);
    if (offset_in_pack != 0)
        throw Exception("offset_in_pack is expected to be zero");
    return Packs(packs.begin() + pack_index, packs.end());
}

void DiskValueSpace::check(const PageReader & meta_page_reader, const String & when)
{
    MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
    serializePacks(buf, packs.begin(), packs.end());
    auto   size        = buf.count();
    char * data_buffer = (char *)::malloc(size);
    SCOPE_EXIT({ ::free(data_buffer); });
    auto read_buf = buf.tryGetReadBuffer();
    read_buf->readStrict(data_buffer, size);
    auto page_checksum = CityHash_v1_0_2::CityHash64(data_buffer, size);
    if (meta_page_reader.getPageChecksum(page_id) != page_checksum)
    {
        auto                 page = meta_page_reader.read(page_id);
        ReadBufferFromMemory rb(page.data.begin(), page.data.size());
        auto                 disk_packs = deserializePacks(rb);
        throw Exception(when + ", DiskValueSpace [" + DB::toString(page_id) + "] memory and disk content not match, memory: "
                        + DB::toString(packs.size()) + ", disk: " + DB::toString(disk_packs.size()));
    }
}

MutableColumnMap DiskValueSpace::cloneCache()
{
    MutableColumnMap clone_cache;
    for (auto & [col_id, col_cache] : cache)
    {
        clone_cache.emplace(col_id, col_cache->cloneResized(col_cache->size()));
    }
    return clone_cache;
}

size_t DiskValueSpace::num_rows() const
{
    size_t rows = 0;
    for (const auto & c : packs)
        rows += c.getRows();
    return rows;
}

size_t DiskValueSpace::num_rows(size_t packs_offset, size_t packs_length) const
{
    size_t rows = 0;
    for (size_t i = packs_offset; i < packs_offset + packs_length; ++i)
        rows += packs.at(i).getRows();
    return rows;
}

size_t DiskValueSpace::num_deletes() const
{
    size_t deletes = 0;
    for (const auto & c : packs)
        deletes += c.isDeleteRange();
    return deletes;
}

size_t DiskValueSpace::num_bytes() const
{
    size_t bytes = 0;
    for (const auto & c : packs)
        bytes += c.getBytes();
    return bytes;
}

size_t DiskValueSpace::num_packs() const
{
    return packs.size();
}

size_t DiskValueSpace::rowsFromBack(size_t pack_num_from_back) const
{
    size_t rows = 0;
    for (ssize_t i = packs.size() - 1; i >= (ssize_t)(packs.size() - pack_num_from_back); --i)
        rows += packs[i].getRows();
    return rows;
}

size_t DiskValueSpace::cacheRows() const
{
    return rowsFromBack(cache_packs);
}

size_t DiskValueSpace::cacheBytes() const
{
    if (cacheRows() && cache.empty())
        throw Exception("cache empty");

    size_t cache_bytes = 0;
    for (const auto & [col_id, col] : cache)
    {
        (void)col_id;
        cache_bytes += col->byteSize();
    }
    return cache_bytes;
}

std::pair<size_t, size_t> DiskValueSpace::findPack(size_t rows_offset) const
{
    size_t count       = 0;
    size_t pack_index = 0;
    for (; pack_index < packs.size(); ++pack_index)
    {
        if (count == rows_offset)
            return {pack_index, 0};
        auto cur_pack_rows = packs[pack_index].getRows();
        count += cur_pack_rows;
        if (count > rows_offset)
            return {pack_index, cur_pack_rows - (count - rows_offset)};
    }
    if (count != rows_offset)
        throw Exception("rows_offset(" + DB::toString(rows_offset) + ") is out of total_rows(" + DB::toString(count) + ")");

    return {pack_index, 0};
}

std::pair<size_t, size_t> DiskValueSpace::findPack(size_t rows_offset, size_t deletes_offset) const
{
    size_t rows_count    = 0;
    size_t deletes_count = 0;
    size_t pack_index   = 0;
    for (; pack_index < packs.size(); ++pack_index)
    {
        if (rows_count == rows_offset && deletes_count == deletes_offset)
            return {pack_index, 0};
        const auto & pack = packs[pack_index];
        if (pack.getRows())
        {
            rows_count += pack.getRows();
            if (rows_count > rows_offset)
            {
                if (unlikely(deletes_count != deletes_offset))
                    throw Exception("deletes_offset and rows_offset are not matched");

                return {pack_index, pack.getRows() - (rows_count - rows_offset)};
            }
        }
        if (pack.isDeleteRange())
        {
            if (deletes_count == deletes_offset)
            {
                if (unlikely(rows_count != rows_offset))
                    throw Exception("deletes_offset and rows_offset are not matched");
                return {pack_index, 0};
            }
            ++deletes_count;
        }
    }
    if (rows_count != rows_offset || deletes_count != deletes_offset)
        throw Exception("illegal rows_offset(" + DB::toString(rows_offset) + "), deletes_count(" + DB::toString(deletes_count) + ")");

    return {pack_index, 0};
}

} // namespace DM
} // namespace DB

#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>

namespace DB::DM
{


std::pair<size_t, size_t> findPack(const DeltaPacks & packs, size_t rows_offset, size_t deletes_offset)
{
    size_t rows_count    = 0;
    size_t deletes_count = 0;
    size_t pack_index    = 0;
    for (; pack_index < packs.size(); ++pack_index)
    {
        if (rows_count == rows_offset && deletes_count == deletes_offset)
            return {pack_index, 0};
        auto & pack = packs[pack_index];

        if (pack->isDeleteRange())
        {
            if (deletes_count == deletes_offset)
            {
                if (unlikely(rows_count != rows_offset))
                    throw Exception("rows_count and rows_offset are expected to be equal. pack_index: " + DB::toString(pack_index)
                                    + ", pack_size: " + DB::toString(packs.size()) + ", rows_count: " + DB::toString(rows_count)
                                    + ", rows_offset: " + DB::toString(rows_offset) + ", deletes_count: " + DB::toString(deletes_count)
                                    + ", deletes_offset: " + DB::toString(deletes_offset));
                return {pack_index, 0};
            }
            ++deletes_count;
        }
        else
        {
            rows_count += pack->getRows();
            if (rows_count > rows_offset)
            {
                if (unlikely(deletes_count != deletes_offset))
                    throw Exception("deletes_count and deletes_offset are expected to be equal. pack_index: " + DB::toString(pack_index)
                                    + ", pack_size: " + DB::toString(packs.size()) + ", rows_count: " + DB::toString(rows_count)
                                    + ", rows_offset: " + DB::toString(rows_offset) + ", deletes_count: " + DB::toString(deletes_count)
                                    + ", deletes_offset: " + DB::toString(deletes_offset));

                return {pack_index, pack->getRows() - (rows_count - rows_offset)};
            }
        }
    }
    if (rows_count != rows_offset || deletes_count != deletes_offset)
        throw Exception("illegal rows_offset and deletes_offset. pack_size: " + DB::toString(packs.size())
                        + ", rows_count: " + DB::toString(rows_count) + ", rows_offset: " + DB::toString(rows_offset)
                        + ", deletes_count: " + DB::toString(deletes_count) + ", deletes_offset: " + DB::toString(deletes_offset));

    return {pack_index, 0};
}

// ================================================
// DeltaValueSpace
// ================================================

DeltaSnapshotPtr DeltaValueSpace::createSnapshot(const DMContext & context, bool for_update, CurrentMetrics::Metric type)
{
    if (for_update && !tryLockUpdating())
        return {};

    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return {};

    auto snap          = std::make_shared<DeltaValueSnapshot>(type);
    snap->is_update    = for_update;
    snap->_delta       = this->shared_from_this();
    snap->storage_snap = std::make_shared<StorageSnapshot>(context.storage_pool, true);
    snap->rows         = rows;
    snap->bytes        = bytes;
    snap->deletes      = deletes;
    snap->packs.reserve(packs.size());

    snap->shared_delta_index = delta_index;

    if (for_update)
    {
        snap->rows -= unsaved_rows;
        snap->bytes -= unsaved_bytes;
        snap->deletes -= unsaved_deletes;
    }

    size_t check_rows    = 0;
    size_t check_deletes = 0;
    size_t total_rows    = 0;
    size_t total_deletes = 0;
    for (const auto & pack : packs)
    {
        // If `for_update` is false, it will create a snapshot with all packs in DeltaValueSpace.
        // If `for_update` is true, only persisted packs are used.
        if (!for_update || pack->isSaved())
        {
            if (auto b = pack->tryToBlock(); (b && b->isCached()))
            {
                // Flush and compact threads could update the value of DeltaPackBlock::cache,
                // and since DeltaPack is not mult-threads safe, we should create a new pack object.
                snap->packs.push_back(std::make_shared<DeltaPackBlock>(*b));
            }
            else
            {
                // For other packs, everything we use is constant, so no need to create a new object.
                snap->packs.push_back(pack);
            }

            check_rows += pack->getRows();
            check_deletes += pack->isDeleteRange();
        }
        total_rows += pack->getRows();
        total_deletes += pack->isDeleteRange();
    }

    if (unlikely(check_rows != snap->rows || check_deletes != snap->deletes || total_rows != rows || total_deletes != deletes))
        throw Exception("Rows and deletes check failed!", ErrorCodes::LOGICAL_ERROR);

    return snap;
}

RowKeyRange DeltaValueSnapshot::getSquashDeleteRange() const
{
    RowKeyRange squashed_delete_range = RowKeyRange::newNone(is_common_handle, rowkey_column_size);
    for (auto iter = packs.cbegin(); iter != packs.cend(); ++iter)
    {
        const auto & pack = *iter;
        if (auto dp_delete = pack->tryToDeleteRange(); dp_delete)
            squashed_delete_range = squashed_delete_range.merge(dp_delete->getDeleteRange());
    }
    return squashed_delete_range;
}

// ================================================
// DeltaValueReader
// ================================================

DeltaValueReader::DeltaValueReader(const DMContext &        context,
                                   const DeltaSnapshotPtr & delta_snap_,
                                   const ColumnDefinesPtr & col_defs_,
                                   const RowKeyRange &      segment_range_)
    : delta_snap(delta_snap_), col_defs(col_defs_), segment_range(segment_range_)
{
    size_t total_rows = 0;
    for (auto & p : delta_snap->getPacks())
    {
        total_rows += p->getRows();
        pack_rows.push_back(p->getRows());
        pack_rows_end.push_back(total_rows);
        pack_readers.push_back(p->getReader(context, delta_snap->getStorageSnapshot(), col_defs));
    }
}

DeltaValueReaderPtr DeltaValueReader::createNewReader(const ColumnDefinesPtr & new_col_defs)
{
    auto new_reader                    = new DeltaValueReader();
    new_reader->delta_snap             = delta_snap;
    new_reader->_compacted_delta_index = _compacted_delta_index;
    new_reader->col_defs               = new_col_defs;
    new_reader->segment_range          = segment_range;
    new_reader->pack_rows              = pack_rows;
    new_reader->pack_rows_end          = pack_rows_end;

    for (auto & pr : pack_readers)
        new_reader->pack_readers.push_back(pr->createNewReader(new_col_defs));

    return std::shared_ptr<DeltaValueReader>(new_reader);
}

size_t DeltaValueReader::readRows(MutableColumns & output_cols, size_t offset, size_t limit, const RowKeyRange * range)
{
    // Note that DeltaMergeBlockInputStream could ask for rows with larger index than total_delta_rows,
    // because DeltaIndex::placed_rows could be larger than total_delta_rows.
    // Here is the example:
    //  1. Thread A create a delta snapshot with 10 rows. Now DeltaValueSnapshot::shared_delta_index->placed_rows == 10.
    //  2. Thread B insert 5 rows into the delta
    //  3. Thread B call Segment::ensurePlace to generate a new DeltaTree, placed_rows = 15, and update DeltaValueSnapshot::shared_delta_index = 15
    //  4. Thread A call Segment::ensurePlace, and DeltaValueReader::shouldPlace will return false. Because placed_rows(15) >= 10
    //  5. Thread A use the DeltaIndex with placed_rows = 15 to do the merge in DeltaMergeBlockInputStream
    //
    // So here, we should filter out those out-of-range rows.

    auto total_delta_rows = delta_snap->getRows();

    auto start = std::min(offset, total_delta_rows);
    auto end   = std::min(offset + limit, total_delta_rows);
    if (end == start)
        return 0;

    auto [start_pack_index, rows_start_in_start_pack] = locatePosByAccumulation(pack_rows_end, start);
    auto [end_pack_index, rows_end_in_end_pack]       = locatePosByAccumulation(pack_rows_end, end);

    size_t actual_read = 0;
    for (size_t pack_index = start_pack_index; pack_index <= end_pack_index; ++pack_index)
    {
        size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
        size_t rows_end_in_pack   = pack_index == end_pack_index ? rows_end_in_end_pack : pack_rows[pack_index];
        size_t rows_in_pack_limit = rows_end_in_pack - rows_start_in_pack;

        // Nothing to read.
        if (rows_start_in_pack == rows_end_in_pack)
            continue;

        auto & pack_reader = pack_readers[pack_index];
        actual_read += pack_reader->readRows(output_cols, rows_start_in_pack, rows_in_pack_limit, range);
    }
    return actual_read;
}

Block DeltaValueReader::readPKVersion(size_t offset, size_t limit)
{
    MutableColumns cols;
    for (size_t i = 0; i < 2; ++i)
        cols.push_back((*col_defs)[i].type->createColumn());
    readRows(cols, offset, limit, nullptr);
    Block block;
    for (size_t i = 0; i < 2; ++i)
    {
        const auto & cd = (*col_defs)[i];
        block.insert(ColumnWithTypeAndName(std::move(cols[i]), cd.type, cd.name, cd.id));
    }
    return block;
}


BlockOrDeletes DeltaValueReader::getPlaceItems(size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end)
{
    /// Note that we merge the consecutive DeltaPackBlock together, which are seperated in groups by DeltaPackDelete and DeltePackFile.

    BlockOrDeletes res;

    auto & packs = delta_snap->getPacks();

    auto [start_pack_index, rows_start_in_start_pack] = findPack(packs, rows_begin, deletes_begin);
    auto [end_pack_index, rows_end_in_end_pack]       = findPack(packs, rows_end, deletes_end);

    size_t block_rows_start = rows_begin;
    size_t block_rows_end   = rows_begin;

    for (size_t pack_index = start_pack_index; pack_index < packs.size() && pack_index <= end_pack_index; ++pack_index)
    {
        auto & pack = *packs[pack_index];

        if (pack.isDeleteRange() || pack.isFile())
        {
            // First, compact the DeltaPackBlocks before this pack into one block.
            if (block_rows_end != block_rows_start)
            {
                auto block = readPKVersion(block_rows_start, block_rows_end - block_rows_start);
                res.emplace_back(std::move(block), block_rows_start);
            }

            // Second, take current pack.
            if (auto pack_delete = pack.tryToDeleteRange(); pack_delete)
            {
                res.emplace_back(pack_delete->getDeleteRange());
            }
            else if (pack.isFile() && pack.getRows())
            {
                auto block = readPKVersion(block_rows_end, pack.getRows());
                res.emplace_back(std::move(block), block_rows_end);
            }

            block_rows_end += pack.getRows();
            block_rows_start = block_rows_end;
        }
        else
        {
            // It is a DeltaPackBlock.
            size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
            size_t rows_end_in_pack   = pack_index == end_pack_index ? rows_end_in_end_pack : pack.getRows();

            block_rows_end += rows_end_in_pack - rows_start_in_pack;

            if (pack_index == packs.size() - 1 || pack_index == end_pack_index)
            {
                // It is the last pack.
                if (block_rows_end != block_rows_start)
                {
                    auto block = readPKVersion(block_rows_start, block_rows_end - block_rows_start);
                    res.emplace_back(std::move(block), block_rows_start);
                }
                block_rows_start = block_rows_end;
            }
        }
    }

    return res;
}

bool DeltaValueReader::shouldPlace(const DMContext &   context,
                                   DeltaIndexPtr       my_delta_index,
                                   const RowKeyRange & segment_range,
                                   const RowKeyRange & relevant_range,
                                   UInt64              max_version)
{
    auto [placed_rows, placed_delete_ranges] = my_delta_index->getPlacedStatus();
    auto & packs                             = delta_snap->getPacks();

    // Already placed.
    if (placed_rows >= delta_snap->getRows() && placed_delete_ranges == delta_snap->getDeletes())
        return false;

    if (relevant_range.all() || relevant_range == segment_range                 //
        || delta_snap->getRows() - placed_rows > context.delta_cache_limit_rows //
        || placed_delete_ranges != delta_snap->getDeletes())
        return true;

    auto [start_pack_index, rows_start_in_start_pack] = locatePosByAccumulation(pack_rows_end, placed_rows);

    for (size_t pack_index = start_pack_index; pack_index < delta_snap->getPackCount(); ++pack_index)
    {
        auto & pack = packs[pack_index];

        // Always do place index if DeltaPackFile exists.
        if (pack->isFile())
            return true;
        if (unlikely(pack->isDeleteRange()))
            throw Exception("pack is delete range", ErrorCodes::LOGICAL_ERROR);

        size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
        size_t rows_end_in_pack   = pack_rows[pack_index];

        auto & pack_reader    = pack_readers[pack_index];
        auto & dpb_reader     = typeid_cast<DPBlockReader &>(*pack_reader);
        auto   pk_column      = dpb_reader.getPKColumn();
        auto   version_column = dpb_reader.getVersionColumn();

        auto   rkcc             = RowKeyColumnContainer(pk_column, context.is_common_handle);
        auto & version_col_data = toColumnVectorData<UInt64>(version_column);

        for (auto i = rows_start_in_pack; i < rows_end_in_pack; ++i)
        {
            if (version_col_data[i] <= max_version && relevant_range.check(rkcc.getRowKeyValue(i)))
                return true;
        }
    }

    return false;
}

} // namespace DB::DM

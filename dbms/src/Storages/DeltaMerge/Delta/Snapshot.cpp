#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>

namespace DB::DM
{
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

    auto snap = std::make_shared<DeltaValueSnapshot>(type);
    snap->is_update = for_update;
    snap->_delta = this->shared_from_this();
    snap->column_stable_file_set_snap = column_stable_file_set->createSnapshot(context);
    snap->shared_delta_index = delta_index;

    if (for_update)
    {
        snap->mem_table_snap = mem_table_set->createSnapshot();
    }
    return snap;
}

RowKeyRange DeltaValueSnapshot::getSquashDeleteRange() const
{
    auto delete_range1 = mem_table_snap->getSquashDeleteRange();
    auto delete_range2 = column_stable_file_set_snap->getSquashDeleteRange();
    return delete_range1.merge(delete_range2);
}

// ================================================
// DeltaValueReader
// ================================================

DeltaValueReader::DeltaValueReader(
    const DMContext & context,
    const DeltaSnapshotPtr & delta_snap_,
    const ColumnDefinesPtr & col_defs_,
    const RowKeyRange & segment_range_)
    : delta_snap(delta_snap_)
    , stable_files_reader()
    , mem_table_reader()
    , col_defs(col_defs_)
    , segment_range(segment_range_)
{}

DeltaValueReaderPtr DeltaValueReader::createNewReader(const ColumnDefinesPtr & new_col_defs)
{
    auto new_reader = new DeltaValueReader();
    new_reader->delta_snap = delta_snap;
    new_reader->compacted_delta_index = compacted_delta_index;
    new_reader->stable_files_reader = stable_files_reader;
    new_reader->mem_table_reader = mem_table_reader;
    new_reader->col_defs = new_col_defs;
    new_reader->segment_range = segment_range;

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
    auto mem_table_rows_offset = delta_snap->getMemTableRowsOffset();
    auto total_delta_rows = delta_snap->getRows();

    auto stable_files_start = std::min(offset, mem_table_rows_offset);
    auto stable_files_end = std::min(offset + limit, mem_table_rows_offset);
    auto mem_table_start = offset <= mem_table_rows_offset ? 0 : std::min(offset - mem_table_rows_offset, total_delta_rows - mem_table_rows_offset);
    auto mem_table_end = offset + limit <= mem_table_rows_offset ? 0 : std::min(offset + limit - mem_table_rows_offset, total_delta_rows - mem_table_rows_offset);

    size_t actual_read = 0;
    if (stable_files_start < stable_files_end)
    {
        actual_read += stable_files_reader->readRows(output_cols, stable_files_start, stable_files_end, range);
    }
    if (mem_table_start < mem_table_end)
    {
        if (unlikely(!mem_table_reader))
            throw Exception("mem table reader shouldn't be empty", ErrorCodes::LOGICAL_ERROR);

        actual_read += mem_table_reader->readRows(output_cols, mem_table_start, mem_table_end, range);
    }
    return actual_read;
}

BlockOrDeletes DeltaValueReader::getPlaceItems(size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end)
{
    /// Note that we merge the consecutive DeltaPackBlock together, which are seperated in groups by DeltaPackDelete and DeltePackFile.

    BlockOrDeletes res;

    auto & packs = delta_snap->getPacks();

    auto [start_pack_index, rows_start_in_start_pack] = findPack(packs, rows_begin, deletes_begin);
    auto [end_pack_index, rows_end_in_end_pack] = findPack(packs, rows_end, deletes_end);

    size_t block_rows_start = rows_begin;
    size_t block_rows_end = rows_begin;

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
            size_t rows_end_in_pack = pack_index == end_pack_index ? rows_end_in_end_pack : pack.getRows();

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

bool DeltaValueReader::shouldPlace(const DMContext & context,
                                   DeltaIndexPtr my_delta_index,
                                   const RowKeyRange & segment_range,
                                   const RowKeyRange & relevant_range,
                                   UInt64 max_version)
{
    auto [placed_rows, placed_delete_ranges] = my_delta_index->getPlacedStatus();
    auto & packs = delta_snap->getPacks();

    // Already placed.
    if (placed_rows >= delta_snap->getRows() && placed_delete_ranges == delta_snap->getDeletes())
        return false;

    if (relevant_range.all() || relevant_range == segment_range //
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
        size_t rows_end_in_pack = pack_rows[pack_index];

        auto & pack_reader = pack_readers[pack_index];
        auto & dpb_reader = typeid_cast<DPBlockReader &>(*pack_reader);
        auto pk_column = dpb_reader.getPKColumn();
        auto version_column = dpb_reader.getVersionColumn();

        auto rkcc = RowKeyColumnContainer(pk_column, context.is_common_handle);
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

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

    auto storage_snap = std::make_shared<StorageSnapshot>(context.storage_pool, context.getReadLimiter(), context.tracing_id, /*snapshot_read*/ true);
    snap->persisted_files_snap = persisted_file_set->createSnapshot(storage_snap);
    snap->shared_delta_index = delta_index;
    snap->mem_table_snap = mem_table_set->createSnapshot(storage_snap, for_update);

    return snap;
}

RowKeyRange DeltaValueSnapshot::getSquashDeleteRange() const
{
    auto delete_range1 = mem_table_snap->getSquashDeleteRange();
    auto delete_range2 = persisted_files_snap->getSquashDeleteRange();
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
    , mem_table_reader(std::make_shared<ColumnFileSetReader>(context, delta_snap_->getMemTableSetSnapshot(), col_defs_, segment_range_))
    , persisted_files_reader(std::make_shared<ColumnFileSetReader>(context, delta_snap_->getPersistedFileSetSnapshot(), col_defs_, segment_range_))
    , col_defs(col_defs_)
    , segment_range(segment_range_)
{}

DeltaValueReaderPtr DeltaValueReader::createNewReader(const ColumnDefinesPtr & new_col_defs)
{
    auto * new_reader = new DeltaValueReader();
    new_reader->delta_snap = delta_snap;
    new_reader->_compacted_delta_index = _compacted_delta_index;
    new_reader->persisted_files_reader = persisted_files_reader->createNewReader(new_col_defs);
    new_reader->mem_table_reader = mem_table_reader->createNewReader(new_col_defs);
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

    const auto mem_table_rows_offset = delta_snap->getMemTableSetRowsOffset();
    const auto total_delta_rows = delta_snap->getRows();

    auto persisted_files_start = std::min(offset, mem_table_rows_offset);
    auto persisted_files_end = std::min(offset + limit, mem_table_rows_offset);
    auto mem_table_start = offset <= mem_table_rows_offset ? 0 : std::min(offset - mem_table_rows_offset, total_delta_rows - mem_table_rows_offset);
    auto mem_table_end = offset + limit <= mem_table_rows_offset ? 0 : std::min(offset + limit - mem_table_rows_offset, total_delta_rows - mem_table_rows_offset);

    size_t actual_read = 0;
    if (persisted_files_start < persisted_files_end)
        actual_read += persisted_files_reader->readRows(output_cols, persisted_files_start, persisted_files_end - persisted_files_start, range);
    if (mem_table_start < mem_table_end)
        actual_read += mem_table_reader->readRows(output_cols, mem_table_start, mem_table_end - mem_table_start, range);

    return actual_read;
}

BlockOrDeletes DeltaValueReader::getPlaceItems(size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end)
{
    /// Note that we merge the consecutive ColumnFileInMemory or ColumnFileTiny together, which are seperated in groups by ColumnFileDeleteRange and ColumnFileBig.
    BlockOrDeletes res;
    auto mem_table_rows_offset = delta_snap->getMemTableSetRowsOffset();
    auto mem_table_deletes_offset = delta_snap->getMemTableSetDeletesOffset();
    auto total_delta_rows = delta_snap->getRows();
    auto total_delta_deletes = delta_snap->getDeletes();

    auto persisted_files_rows_begin = std::min(rows_begin, mem_table_rows_offset);
    auto persisted_files_deletes_begin = std::min(deletes_begin, mem_table_deletes_offset);
    auto persisted_files_rows_end = std::min(rows_end, mem_table_rows_offset);
    auto persisted_files_deletes_end = std::min(deletes_end, mem_table_deletes_offset);

    auto mem_table_rows_begin = rows_begin <= mem_table_rows_offset ? 0 : std::min(rows_begin - mem_table_rows_offset, total_delta_rows - mem_table_rows_offset);
    auto mem_table_deletes_begin = deletes_begin <= mem_table_deletes_offset ? 0 : std::min(deletes_begin - mem_table_deletes_offset, total_delta_deletes - mem_table_deletes_offset);
    auto mem_table_rows_end = rows_end <= mem_table_rows_offset ? 0 : std::min(rows_end - mem_table_rows_offset, total_delta_rows - mem_table_rows_offset);
    auto mem_table_deletes_end = deletes_end <= mem_table_deletes_offset ? 0 : std::min(deletes_end - mem_table_deletes_offset, total_delta_deletes - mem_table_deletes_offset);

    persisted_files_reader->getPlaceItems(res, persisted_files_rows_begin, persisted_files_deletes_begin, persisted_files_rows_end, persisted_files_deletes_end);
    mem_table_reader->getPlaceItems(res, mem_table_rows_begin, mem_table_deletes_begin, mem_table_rows_end, mem_table_deletes_end, mem_table_rows_offset);

    return res;
}

bool DeltaValueReader::shouldPlace(const DMContext & context,
                                   DeltaIndexPtr my_delta_index,
                                   const RowKeyRange & segment_range_,
                                   const RowKeyRange & relevant_range,
                                   UInt64 max_version)
{
    auto [placed_rows, placed_delete_ranges] = my_delta_index->getPlacedStatus();

    // Already placed.
    if (placed_rows >= delta_snap->getRows() && placed_delete_ranges == delta_snap->getDeletes())
        return false;

    if (relevant_range.all() || relevant_range == segment_range_ //
        || delta_snap->getRows() - placed_rows > context.delta_cache_limit_rows //
        || placed_delete_ranges != delta_snap->getDeletes())
        return true;

    size_t rows_in_persisted_file_snap = delta_snap->getMemTableSetRowsOffset();
    return persisted_files_reader->shouldPlace(context, relevant_range, max_version, placed_rows)
        || mem_table_reader->shouldPlace(context, relevant_range, max_version, placed_rows <= rows_in_persisted_file_snap ? 0 : placed_rows - rows_in_persisted_file_snap);
}

} // namespace DB::DM

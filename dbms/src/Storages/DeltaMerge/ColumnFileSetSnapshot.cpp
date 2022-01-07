#include "ColumnFileSetSnapshot.h"

namespace DB
{
namespace DM
{

ColumnFileSetReader::ColumnFileSetReader(
    const DMContext & context,
    const ColumnFileSetSnapshotPtr & snapshot_,
    const ColumnDefinesPtr & col_defs_,
    const RowKeyRange & segment_range_)
    : snapshot(snapshot_)
    , col_defs(col_defs_)
    , segment_range(segment_range_)
{
    size_t total_rows = 0;
    for (auto & f : snapshot->getColumnFiles())
    {
        total_rows += f->getRows();
        column_file_rows.push_back(f->getRows());
        column_file_rows_end.push_back(total_rows);
        column_file_readers.push_back(f->getReader(context, snapshot->getStorageSnapshot(), col_defs));
    }
}

size_t ColumnFileSetReader::readRows(MutableColumns & output_columns, size_t offset, size_t limit, const RowKeyRange * range)
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

    auto total_delta_rows = snapshot->getRows();

    auto start = std::min(offset, total_delta_rows);
    auto end = std::min(offset + limit, total_delta_rows);
    if (end == start)
        return 0;

    auto [start_pack_index, rows_start_in_start_pack] = locatePosByAccumulation(column_file_rows_end, start);
    auto [end_pack_index, rows_end_in_end_pack] = locatePosByAccumulation(column_file_rows_end, end);

    size_t actual_read = 0;
    for (size_t pack_index = start_pack_index; pack_index <= end_pack_index; ++pack_index)
    {
        size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
        size_t rows_end_in_pack = pack_index == end_pack_index ? rows_end_in_end_pack : column_file_rows[pack_index];
        size_t rows_in_pack_limit = rows_end_in_pack - rows_start_in_pack;

        // Nothing to read.
        if (rows_start_in_pack == rows_end_in_pack)
            continue;

        auto & column_file_reader = column_file_readers[pack_index];
        actual_read += column_file_reader->readRows(output_columns, rows_start_in_pack, rows_in_pack_limit, range);
    }
    return actual_read;
}

}
}

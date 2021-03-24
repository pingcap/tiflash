#include <DataStreams/ConcatBlockInputStream.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
namespace DM
{

constexpr double APPROX_DELTA_THRESHOLD = 0.1;

RowsAndBytes Segment::getRowsAndBytesInRange(DMContext &                dm_context,
                                             const SegmentSnapshotPtr & segment_snap,
                                             const RowKeyRange &        check_range,
                                             bool                       is_exact)
{
    RowKeyRange real_range = rowkey_range.shrink(check_range);
    // If there are no delete ranges overlap with check_range, and delta is small
    // then we simply return the estimated stat of stable.
    // Otherwise, use the exact version.
    if (!is_exact && (segment_snap->delta->getRows() <= segment_snap->stable->getRows() * APPROX_DELTA_THRESHOLD))
    {
        bool has_delete_range = false;

        auto & packs = segment_snap->delta->getPacks();
        for (auto & pack : packs)
        {
            if (auto dp_delete = pack->tryToDeleteRange(); dp_delete && real_range.intersect(dp_delete->getDeleteRange()))
            {
                has_delete_range = true;
                break;
            }
        }
        // Only counts data in stable, because delta is expected to be small.
        if (!has_delete_range)
            return segment_snap->stable->getApproxRowsAndBytes(dm_context, real_range);
    }

    // Otherwise, we have to use the (nearly) exact version.
    Float64 avg_row_bytes = (segment_snap->delta->getBytes() + segment_snap->stable->getBytes())
        / (segment_snap->delta->getRows() + segment_snap->stable->getRows());

    auto & handle    = getExtraHandleColumnDefine(is_common_handle);
    auto & version   = getVersionColumnDefine();
    auto   read_info = getReadInfo(dm_context, {handle, version}, segment_snap, {real_range});

    auto storage_snap = std::make_shared<StorageSnapshot>(dm_context.storage_pool);
    auto pk_ver_col_defs
        = std::make_shared<ColumnDefines>(ColumnDefines{getExtraHandleColumnDefine(dm_context.is_common_handle), getVersionColumnDefine()});
    auto delta_reader = std::make_shared<DeltaValueReader>(dm_context, segment_snap->delta, pk_ver_col_defs, this->rowkey_range);

    size_t exact_rows = 0;
    {
        BlockInputStreamPtr data_stream = getPlacedStream(dm_context,
                                                          *read_info.read_columns,
                                                          real_range,
                                                          EMPTY_FILTER,
                                                          segment_snap->stable,
                                                          delta_reader,
                                                          read_info.index_begin,
                                                          read_info.index_end,
                                                          dm_context.stable_pack_rows);

        data_stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(data_stream, rowkey_range, 0);
        data_stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
            data_stream, *read_info.read_columns, dm_context.min_version, is_common_handle);

        data_stream->readPrefix();
        Block block;
        while ((block = data_stream->read()))
            exact_rows += block.rows();
        data_stream->readSuffix();
    }

    return {exact_rows, exact_rows * avg_row_bytes};
}

RowsAndBytes DeltaMergeStore::getRowsAndBytesInRange(const Context & db_context, const RowKeyRange & check_range, bool is_exact)
{
    auto dm_context = newDMContext(db_context, db_context.getSettingsRef());
    return getRowsAndBytesInRange(*dm_context, check_range, is_exact);
}

RowsAndBytes DeltaMergeStore::getRowsAndBytesInRange(DMContext & dm_context, const RowKeyRange & check_range, bool is_exact)
{
    auto tasks = getReadTasksByRanges(dm_context, {check_range});

    size_t rows  = 0;
    size_t bytes = 0;
    while (!tasks.empty())
    {
        auto task = tasks.front();
        tasks.pop_front();
        auto [_rows, _bytes] = task->segment->getRowsAndBytesInRange(dm_context, task->read_snapshot, check_range, is_exact);
        rows += _rows;
        bytes += _bytes;
    }

    LOG_DEBUG(log,
              __FUNCTION__ << " [range:" << check_range.toDebugString() << "] [is_exact:" << is_exact << "] [rows:" << rows
                           << "] [bytes:" << bytes << "]");

    return {rows, bytes};
}

RegionSplitRes
DeltaMergeStore::getRegionSplitPoint(const Context & db_context, const RowKeyRange & check_range, size_t max_region_size, size_t split_size)
{
    auto dm_context = newDMContext(db_context, db_context.getSettingsRef());
    return getRegionSplitPoint(*dm_context, check_range, max_region_size, split_size);
}

/// Get the split point of region with check_range.
/// Currently only do half split.
RegionSplitRes
DeltaMergeStore::getRegionSplitPoint(DMContext & dm_context, const RowKeyRange & check_range, size_t max_region_size, size_t split_size)
{
    auto [approx_rows, approx_bytes] = getRowsAndBytesInRange(dm_context, check_range, /*is_exact*/ false);
    (void)approx_bytes;

    size_t check_step = std::max(1, approx_rows / dm_context.region_split_check_points);

    std::vector<RowKeyValue> check_points;
    check_points.reserve(dm_context.region_split_check_points);

    size_t exact_rows  = 0;
    size_t exact_bytes = 0;
    {
        auto tasks = getReadTasksByRanges(dm_context, {check_range});

        size_t total_rows  = 0;
        size_t total_bytes = 0;
        for (auto & task : tasks)
        {
            auto [rows, bytes] = task->getRowsAndBytes();
            total_rows += rows;
            total_bytes += bytes;
        }

        BlockInputStreams streams;
        while (!tasks.empty())
        {
            auto task = tasks.front();
            tasks.pop_front();
            // Note that handle, version and tag columns will be loaded even if we do not specify here.
            streams.push_back(task->segment->getInputStreamForDataExport(dm_context, /*columns*/ {}, task->read_snapshot, check_range));
        }
        ConcatBlockInputStream stream(streams);

        stream.readPrefix();
        size_t next_index = check_step;
        while (true)
        {
            Block block = stream.read();
            if (!block)
                break;

            auto                  block_rows = block.rows();
            RowKeyColumnContainer rkc(block.getByPosition(0).column, is_common_handle);
            while (next_index < block_rows)
            {
                check_points.push_back(rkc.getRowKeyValue(next_index).toRowKeyValue());
                next_index += check_step;
            }
            next_index -= block_rows;

            exact_rows += block_rows;
        }

        stream.readSuffix();

        exact_bytes = exact_rows * ((Float64)total_bytes / total_rows);
    }

    /// Don't split if region is not too big.
    if (exact_rows < max_region_size || check_points.empty())
        return RegionSplitRes{.split_points = {}, .exact_rows = exact_rows, .exact_bytes = exact_bytes};

    auto split_point = check_points[check_points.size() / 2];

    LOG_DEBUG(log,
              __FUNCTION__ << "check_range:" << check_range.toDebugString() << "] [max_region_size:" << max_region_size
                           << "] [split_size:" << split_size << "] [split_point:" << split_point.toDebugString() << "]");

    return RegionSplitRes{.split_points = {split_point}, .exact_rows = exact_rows, .exact_bytes = exact_bytes};
}

} // namespace DM
} // namespace DB

#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Core/SortDescription.h>
#include <Functions/FunctionsConversion.h>
#include <Interpreters/sortBlock.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMDecoratorStreams.h>
#include <Storages/DeltaMerge/DMSegmentThreadInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

DeltaMergeStore::DeltaMergeStore(Context &             db_context,
                                 const String &        path_,
                                 const String &        name,
                                 const ColumnDefines & columns,
                                 const ColumnDefine &  handle,
                                 const Settings &      settings_)
    : path(path_),
      storage_pool(path),
      table_name(name),
      table_handle_define(handle),
      background_pool(db_context.getBackgroundPool()),
      settings(settings_)
{
    // We use Int64 to store handle.
    if (!table_handle_define.type->equals(*EXTRA_HANDLE_COLUMN_TYPE))
    {
        table_handle_original_type = table_handle_define.type;
        table_handle_define.type   = EXTRA_HANDLE_COLUMN_TYPE;
    }

    table_columns.emplace_back(table_handle_define);
    table_columns.emplace_back(VERSION_COLUMN_DEFINE);
    table_columns.emplace_back(TAG_COLUMN_DEFINE);

    for (auto & col : columns)
    {
        if (col.name != table_handle_define.name && col.name != VERSION_COLUMN_NAME && col.name != TAG_COLUMN_NAME)
            table_columns.emplace_back(col);
    }

    DMContext dm_context = newDMContext(db_context, db_context.getSettingsRef());
    if (!storage_pool.maxMetaPageId())
    {
        // Create the first segment.
        auto segment_id = storage_pool.newMetaPageId();
        if (segment_id != DELTA_MERGE_FIRST_SEGMENT_ID)
            throw Exception("The first segment id should be " + DB::toString(DELTA_MERGE_FIRST_SEGMENT_ID), ErrorCodes::LOGICAL_ERROR);
        auto first_segment = Segment::newSegment(dm_context, HandleRange::newAll(), segment_id, 0);
        segments.emplace(first_segment->getRange().end, first_segment);
    }
    else
    {
        auto segment_id = DELTA_MERGE_FIRST_SEGMENT_ID;
        while (segment_id)
        {
            auto segment = Segment::restoreSegment(dm_context, segment_id);
            segments.emplace(segment->getRange().end, segment);
            segment_id = segment->nextSegmentId();
        }
    }

    gc_handle = background_pool.addTask([this] { return storage_pool.gc(); });
}

DeltaMergeStore::~DeltaMergeStore()
{
    background_pool.removeTask(gc_handle);
}

void DeltaMergeStore::write(const Context & db_context, const DB::Settings & db_settings, const Block & to_write)
{
    std::unique_lock lock(mutex);

    size_t rows = to_write.rows();
    if (!rows)
        return;

    DMContext dm_context = newDMContext(db_context, db_settings);
    Block     block      = to_write;

    const auto & handle_define = table_handle_define;
    {
        // Transform handle column into Int64.
        auto handle_pos = block.getPositionByName(handle_define.name);
        if (!block.getByPosition(handle_pos).type->equals(*EXTRA_HANDLE_COLUMN_TYPE))
        {
            FunctionToInt64::create(db_context)->execute(block, {handle_pos}, handle_pos);
            block.getByPosition(handle_pos).type = EXTRA_HANDLE_COLUMN_TYPE;
        }
    }

    {
        // Sort by handle & version in ascending order.
        SortDescription sort;
        sort.emplace_back(handle_define.name, 1, 0);
        sort.emplace_back(VERSION_COLUMN_NAME, 1, 0);

        if (!isAlreadySorted(block, sort))
        {
            IColumn::Permutation perm;
            stableGetPermutation(block, sort, perm);

            for (size_t i = 0; i < block.columns(); ++i)
            {
                auto & c = block.getByPosition(i);
                c.column = c.column->permute(perm, 0);
            }
        }
    }

    const auto & handle_data = getColumnVectorData<Handle>(block, block.getPositionByName(handle_define.name));

    Handle start          = handle_data[0];
    auto   cur_segment_it = segments.upper_bound(start);
    if (cur_segment_it == segments.end())
    {
        if (start == MAX_INT64)
            cur_segment_it--;
        else
            throw Exception("Failed to find segment with proper range", ErrorCodes::LOGICAL_ERROR);
    }

    auto write = [&](const SegmentPtr & segment, size_t offset, size_t num) {
        Block sub_block;
        for (const auto & c : block)
        {
            auto column = c.column->cloneEmpty();
            column->insertRangeFrom(*c.column, offset, num);
            auto sub_col      = c.cloneEmpty();
            sub_col.column    = std::move(column);
            sub_col.column_id = c.column_id;
            sub_block.insert(std::move(sub_col));
        }
        segment->write(dm_context, std::move(sub_block));
    };

    // Prepare segment updates.
    auto cur_seg_end = cur_segment_it->first;
    if (handle_data[rows - 1] < cur_seg_end)
    {
        // Current segment includes the all data in the block.
        write(cur_segment_it->second, 0, rows);
    }
    else
    {
        size_t from   = 0;
        size_t row_id = 0;
        for (; row_id < rows; ++row_id)
        {
            Handle handle = handle_data[row_id];
            // A segment with P_INF_Handle is the last segment.
            if (handle >= cur_seg_end && cur_seg_end != P_INF_HANDLE)
            {
                // Split from here.
                write(cur_segment_it->second, from, row_id - from);

                from = row_id;
                ++cur_segment_it;
                cur_seg_end = cur_segment_it->first;
            }
        }
        if (cur_segment_it == segments.end())
            throw Exception("No more sutable segment");
        write(cur_segment_it->second, from, row_id - from);
    }

    // This should be called by background thread.
    checkAll(db_context, db_settings);
}


BlockInputStreams DeltaMergeStore::read(const Context &       db_context,
                                        const DB::Settings &  db_settings,
                                        const ColumnDefines & columns_to_read,
                                        size_t                expected_block_size,
                                        size_t                num_streams,
                                        UInt64                max_version,
                                        bool                  is_raw)
{
    std::shared_lock lock(mutex);

    auto dm_context = newDMContext(db_context, db_settings);

    ColumnDefines new_columns_to_read;
    const auto &  handle_define = table_handle_define;

    new_columns_to_read.push_back(handle_define);
    new_columns_to_read.push_back(VERSION_COLUMN_DEFINE);
    new_columns_to_read.push_back(TAG_COLUMN_DEFINE);

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        const auto & c = columns_to_read[i];
        if (c.id != handle_define.id && c.id != VERSION_COLUMN_ID && c.id != TAG_COLUMN_ID)
            new_columns_to_read.push_back(c);
    }

    auto stream_creator = [=](const SegmentPtr & segment) {
        return segment->getInputStream(dm_context, new_columns_to_read, expected_block_size, max_version, is_raw);
    };

    Segments to_read_segments;
    for (const auto & [handle, segment] : segments)
    {
        (void)handle;
        to_read_segments.emplace_back(segment);
    }
    auto read_task_pool = std::make_shared<SegmentReadTaskPool>(createHeader(new_columns_to_read), to_read_segments, stream_creator);

    BlockInputStreams res;
    for (size_t i = 0; i < num_streams && i < segments.size(); ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>(read_task_pool);
        if (!is_raw)
            stream = std::make_shared<DMHandleConvertBlockInputStream>(stream, handle_define.name, table_handle_original_type, db_context);
        stream = std::make_shared<DMColumnFilterBlockInputStream>(stream, columns_to_read);

        res.push_back(stream);
    }
    return res;
}

bool DeltaMergeStore::checkAll(const Context & db_context, const DB::Settings & db_settings)
{
    auto   dm_context           = newDMContext(db_context, db_settings);
    size_t segment_rows_setting = db_settings.dm_segment_rows;

    for (const auto & [end, segment] : segments)
    {
        (void)end;
        bool res = checkSplitOrMerge(segment, dm_context, segment_rows_setting);
        if (res)
            return true;
    }
    return false;
}

bool DeltaMergeStore::checkSplitOrMerge(const SegmentPtr & segment, DMContext dm_context, size_t segment_rows_setting)
{
    /// TODO: fix this naive algorithm, to reduce merge/split frequency.
    size_t segment_rows = segment->getEstimatedRows();
    if (segment_rows > segment_rows_setting * 2)
    {
        split(dm_context, segment);
        return true;
    }
    else if (segment_rows < segment_rows_setting / 2)
    {
        // TODO: We should not only check the next segment!!!
        auto it = segments.find(segment->getRange().end);
        ++it;
        if (it != segments.end())
        {
            auto   next_segment = it->second;
            size_t next_rows    = next_segment->getEstimatedRows();
            if (segment_rows + next_rows <= segment_rows_setting)
            {
                merge(dm_context, segment, next_segment);
                return true;
            }
        }
    }
    return false;
}

void DeltaMergeStore::split(DMContext & dm_context, SegmentPtr segment)
{
    auto next_segment = segment->split(dm_context);

    segments[segment->getRange().end]      = segment;
    segments[next_segment->getRange().end] = next_segment;
}

void DeltaMergeStore::merge(DMContext & dm_context, SegmentPtr left, SegmentPtr right)
{
    auto left_range  = left->getRange();
    auto right_range = right->getRange();

    left->merge(dm_context, right);

    segments.erase(left_range.end);
    segments[right_range.end] = left;
}

} // namespace DB
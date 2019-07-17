#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Core/SortDescription.h>
#include <Functions/FunctionsConversion.h>
#include <Interpreters/sortBlock.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMSegmentThreadInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace ProfileEvents
{
extern const Event DMWriteBlock;
extern const Event DMWriteBlockNS;
} // namespace ProfileEvents

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

namespace DM
{

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
      settings(settings_),
      log(&Logger::get("DeltaMergeStore"))
{
    // We use Int64 to store handle.
    if (!table_handle_define.type->equals(*EXTRA_HANDLE_COLUMN_TYPE))
    {
        table_handle_real_type   = table_handle_define.type;
        table_handle_define.type = EXTRA_HANDLE_COLUMN_TYPE;
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

    EventRecorder recorder(ProfileEvents::DMWriteBlock, ProfileEvents::DMWriteBlockNS);

    const size_t rows = to_write.rows();
    if (rows == 0)
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
            stableSortBlock(block, sort);
    }

    if (log->debug())
    {
        String msg = "Before insert block. All segments:{";
        for (auto & [end, segment] : segments)
        {
            (void)end;
            msg += DB::toString(segment->segmentId()) + ":" + segment->getRange().toString() + ",";
        }
        msg.pop_back();
        msg += "}";
        LOG_DEBUG(log, msg);
    }

    const auto & handle_data = getColumnVectorData<Handle>(block, block.getPositionByName(handle_define.name));

    size_t offset = 0;
    while (offset != rows)
    {
        auto start      = handle_data[offset];
        auto segment_it = segments.upper_bound(start);
        if (segment_it == segments.end())
        {
            if (start == P_INF_HANDLE)
                --segment_it;
            else
                throw Exception("Failed to locate segment begin with start: " + DB::toString(start), ErrorCodes::LOGICAL_ERROR);
        }
        auto segment = segment_it->second;
        auto range   = segment->getRange();
        auto end_pos = range.end == P_INF_HANDLE ? handle_data.cend()
                                                 : std::lower_bound(handle_data.cbegin() + offset, handle_data.cend(), range.end);
        size_t limit = end_pos - (handle_data.cbegin() + offset);

        LOG_DEBUG(log,
                  "Insert block. Segment range " + segment->getRange().toString() + //
                      ", block range " + rangeToString(offset, offset + limit));
        write_segment(dm_context, segment, block, offset, limit);

        offset += limit;
    }

    // This should be called by background thread.
    afterInsertOrDelete(db_context, db_settings);

    recorder.submit();
}

void DeltaMergeStore::write_segment(DMContext &        dm_context, //
                                    const SegmentPtr & segment,
                                    const Block &      block,
                                    size_t             offset,
                                    size_t             limit)
{
    if (!offset && limit == block.rows())
    {
        Block block_copy = block;
        segment->write(dm_context, std::move(block_copy));
    }
    else
    {
        Block sub_block;
        for (const auto & c : block)
        {
            auto column = c.column->cloneEmpty();
            column->insertRangeFrom(*c.column, offset, limit);

            auto sub_col      = c.cloneEmpty();
            sub_col.column    = std::move(column);
            sub_col.column_id = c.column_id;
            sub_block.insert(std::move(sub_col));
        }
        segment->write(dm_context, std::move(sub_block));
    }
}

BlockInputStreams DeltaMergeStore::readRaw(const Context &       db_context,
                                           const DB::Settings &  db_settings,
                                           const ColumnDefines & columns_to_read,
                                           size_t                num_streams)
{
    std::shared_lock lock(mutex);

    auto dm_context     = newDMContext(db_context, db_settings);
    auto stream_creator = [=](const SegmentReadTask & task) { return task.segment->getInputStreamRaw(dm_context, columns_to_read); };

    SegmentReadTasks tasks;
    for (const auto & [handle, segment] : segments)
    {
        (void)handle;
        tasks.emplace_back(SegmentReadTask(segment, {segment->getRange()}));
    }

    auto read_task_pool = std::make_shared<SegmentReadTaskPool>(std::move(tasks), stream_creator);

    BlockInputStreams res;
    for (size_t i = 0; i < num_streams && i < segments.size(); ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>( //
            read_task_pool,
            columns_to_read,
            table_handle_define.name,
            DataTypePtr{},
            db_context);

        res.push_back(stream);
    }
    return res;
}

BlockInputStreams DeltaMergeStore::read(const Context &       db_context,
                                        const DB::Settings &  db_settings,
                                        const ColumnDefines & columns_to_read,
                                        const HandleRanges &  sorted_ranges,
                                        size_t                num_streams,
                                        UInt64                max_version,
                                        size_t                expected_block_size)
{
    std::shared_lock lock(mutex);

    auto range_it = sorted_ranges.begin();
    auto seg_it   = segments.upper_bound(range_it->start);

    if (seg_it == segments.end())
    {
        if (range_it->start == P_INF_HANDLE)
            --seg_it;
        else
            throw Exception("Failed to locate segment begin with start: " + DB::toString(range_it->start), ErrorCodes::LOGICAL_ERROR);
    }

    SegmentReadTasks tasks;
    while (range_it != sorted_ranges.end() && seg_it != segments.end())
    {
        auto & req_range = *range_it;
        auto & seg_range = seg_it->second->getRange();
        if (req_range.intersect(seg_range))
        {
            if (tasks.empty() || tasks.back().segment != seg_it->second)
                tasks.emplace_back(seg_it->second);

            tasks.back().addRange(req_range);

            if (req_range.end < seg_range.end)
            {
                ++range_it;
            }
            else if (req_range.end > seg_range.end)
            {
                ++seg_it;
            }
            else
            {
                ++range_it;
                ++seg_it;
            }
        }
        else
        {
            if (req_range.end < seg_range.start)
                ++range_it;
            else
                ++seg_it;
        }
    }

    auto dm_context = newDMContext(db_context, db_settings);

    auto stream_creator = [=](const SegmentReadTask & task) {
        return task.segment->getInputStream(
            dm_context, columns_to_read, task.ranges, max_version, std::min(expected_block_size, DEFAULT_BLOCK_SIZE));
    };

    auto read_task_pool = std::make_shared<SegmentReadTaskPool>(std::move(tasks), stream_creator);

    BlockInputStreams res;
    for (size_t i = 0; i < num_streams && i < segments.size(); ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>( //
            read_task_pool,
            columns_to_read,
            table_handle_define.name,
            table_handle_real_type,
            db_context);

        res.push_back(stream);
    }
    return res;
}

bool DeltaMergeStore::afterInsertOrDelete(const Context & db_context, const DB::Settings & db_settings)
{
    auto   dm_context           = newDMContext(db_context, db_settings);
    size_t segment_rows_setting = db_settings.dm_segment_rows;

    bool is_continue = true;
    while (is_continue)
    {
        for (const auto & [end, segment] : segments)
        {
            (void)end;
            is_continue = checkSplitOrMerge(segment, dm_context, segment_rows_setting);
            if (is_continue)
                break;
        }
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
    LOG_DEBUG(log, "Split segment " + segment->info());

    auto range = segment->getRange();

    auto next_segment = segment->split(dm_context);

    segments.erase(range.end);
    segments[segment->getRange().end]      = segment;
    segments[next_segment->getRange().end] = next_segment;

#ifndef NDEBUG
    segment->check(dm_context, "After split(original). " + segment->simpleInfo());
    next_segment->check(dm_context, "After split(new). " + next_segment->simpleInfo());
#endif
}

void DeltaMergeStore::merge(DMContext & dm_context, SegmentPtr left, SegmentPtr right)
{
    LOG_DEBUG(log, "Merge segment, left:" + left->info() + ", right:" + right->info());

    auto left_range  = left->getRange();
    auto right_range = right->getRange();

    left->merge(dm_context, right);

    segments.erase(left_range.end);
    segments[right_range.end] = left;

#ifndef NDEBUG
    left->check(dm_context, "After merge. " + left->simpleInfo());
#endif
}

void DeltaMergeStore::check(const Context & db_context, const DB::Settings & db_settings)
{
    auto dm_context = newDMContext(db_context, db_settings);

    for (const auto & [end, segment] : segments)
    {
        (void)end;
        segment->check(dm_context, "Manually");
    }
}

} // namespace DM
} // namespace DB
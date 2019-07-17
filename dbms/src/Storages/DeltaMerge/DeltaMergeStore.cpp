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
        std::shared_lock lock(mutex);

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

    struct WriteAction
    {
        SegmentPtr segment;
        size_t     offset;
        size_t     limit;
    };
    std::vector<WriteAction> actions;

    {
        std::shared_lock lock(mutex);

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

            actions.emplace_back(WriteAction{segment, offset, limit});

            offset += limit;
        }
    }

    for (auto & action : actions)
    {
        LOG_DEBUG(log,
                  "Insert block. Segment range " + action.segment->getRange().toString() + //
                      ", block range " + rangeToString(action.offset, action.offset + action.limit));
        auto range_end   = action.segment->getRange().end;
        auto new_semgent = write_segment(dm_context, action.segment, block, action.offset, action.limit);
        if (new_semgent)
        {
            std::unique_lock lock(mutex);

            segments[range_end] = new_semgent;
        }
    }

    // This should be called by background thread.
    afterInsertOrDelete(db_context, db_settings);

    recorder.submit();
}

SegmentPtr DeltaMergeStore::write_segment(DMContext &        dm_context, //
                                          const SegmentPtr & segment,
                                          const Block &      block,
                                          size_t             offset,
                                          size_t             limit)
{
    if (!offset && limit == block.rows())
    {
        Block block_copy = block;
        return segment->write(dm_context, std::move(block_copy));
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
        return segment->write(dm_context, std::move(sub_block));
    }
}

BlockInputStreams DeltaMergeStore::readRaw(const Context &       db_context,
                                           const DB::Settings &  db_settings,
                                           const ColumnDefines & columns_to_read,
                                           size_t                num_streams)
{
    SegmentReadTasks tasks;
    {
        std::shared_lock lock(mutex);

        for (const auto & [handle, segment] : segments)
        {
            (void)handle;
            tasks.emplace_back(SegmentReadTask(segment, {segment->getRange()}));
        }
    }

    auto dm_context     = newDMContext(db_context, db_settings);
    auto stream_creator = [=](const SegmentReadTask & task) { return task.segment->getInputStreamRaw(dm_context, columns_to_read); };

    size_t final_num_stream = std::min(num_streams, tasks.size());
    auto   read_task_pool   = std::make_shared<SegmentReadTaskPool>(std::move(tasks), stream_creator);

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
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
    SegmentReadTasks tasks;

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
    }

    auto dm_context = newDMContext(db_context, db_settings);

    auto stream_creator = [=](const SegmentReadTask & task) {
        return task.segment->getInputStream(
            dm_context, columns_to_read, task.ranges, max_version, std::min(expected_block_size, DEFAULT_BLOCK_SIZE));
    };

    size_t final_num_stream = std::min(num_streams, tasks.size());
    auto   read_task_pool   = std::make_shared<SegmentReadTaskPool>(std::move(tasks), stream_creator);

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
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

    while (true)
    {
        /// TODO: fix this naive algorithm, to reduce merge/split frequency.

        SegmentPtr option;
        SegmentPtr next;
        bool       is_split;

        {
            std::shared_lock lock(mutex);

            auto it = segments.begin();
            while (it != segments.end())
            {
                auto & segment = it->second;
                if (shouldSplit(segment, segment_rows_setting))
                {
                    option   = segment;
                    is_split = true;
                    break;
                }
                ++it;
                if (it == segments.end())
                    break;
                auto & next_segment = it->second;
                if (shouldMerge(segment, next_segment, segment_rows_setting))
                {
                    option   = segment;
                    next     = next_segment;
                    is_split = false;
                    break;
                }
            }
        }

        if (option)
        {
            if (is_split)
            {
                split(dm_context, option);
            }
            else
            {
                merge(dm_context, option, next);
            }
            // Keep checking until there are no options any more.
            continue;
        }
        else
        {
            // No more options, break.
            break;
        }
    }
    return false;
}


bool DeltaMergeStore::shouldSplit(const SegmentPtr & segment, size_t segment_rows_setting)
{
    size_t segment_rows = segment->getEstimatedRows();
    return segment_rows > segment_rows_setting * 2;
}

bool DeltaMergeStore::shouldMerge(const SegmentPtr & left, const SegmentPtr & right, size_t segment_rows_setting)
{
    size_t segment_rows = left->getEstimatedRows();
    if (segment_rows < segment_rows_setting / 2)
    {
        size_t next_rows = right->getEstimatedRows();
        if (segment_rows + next_rows <= segment_rows_setting)
            return true;
    }
    return false;
}

void DeltaMergeStore::split(DMContext & dm_context, const SegmentPtr & segment)
{
    LOG_DEBUG(log, "Split segment " + segment->info());

    auto range                         = segment->getRange();
    auto [new_seg_left, new_seg_right] = segment->split(dm_context);

    {
        std::unique_lock lock(mutex);

        segments.erase(range.end);
        segments[new_seg_left->getRange().end]  = new_seg_left;
        segments[new_seg_right->getRange().end] = new_seg_right;
    }

#ifndef NDEBUG
    new_seg_left->check(dm_context, "After split(left). " + new_seg_left->simpleInfo());
    new_seg_right->check(dm_context, "After split(right). " + new_seg_right->simpleInfo());
#endif
}

void DeltaMergeStore::merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right)
{
    LOG_DEBUG(log, "Merge segment, left:" + left->info() + ", right:" + right->info());

    auto left_range  = left->getRange();
    auto right_range = right->getRange();

    auto merged = Segment::merge(dm_context, left, right);

    {
        std::unique_lock lock(mutex);

        segments.erase(left_range.end);
        segments.erase(right_range.end);
        segments.emplace(merged->getRange().end, merged);
    }

#ifndef NDEBUG
    merged->check(dm_context, "After merge. " + merged->simpleInfo());
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
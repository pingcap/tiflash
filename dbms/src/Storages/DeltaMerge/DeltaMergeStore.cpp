#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Core/SortDescription.h>
#include <Functions/FunctionsConversion.h>
#include <Interpreters/sortBlock.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMSegmentThreadInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeStore-internal.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace ProfileEvents
{
extern const Event DMWriteBlock;
extern const Event DMWriteBlockNS;
extern const Event DMDeleteRange;
extern const Event DMDeleteRangeNS;
extern const Event DMAppendDeltaCommitDisk;
extern const Event DMAppendDeltaCommitDiskNS;
extern const Event DMAppendDeltaCleanUp;
extern const Event DMAppendDeltaCleanUpNS;
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
                                 const String &        db_name,
                                 const String &        table_name_,
                                 const ColumnDefines & columns,
                                 const ColumnDefine &  handle,
                                 const Settings &      settings_)
    : path(path_),
      storage_pool(db_name + "." + table_name_, path),
      table_name(table_name_),
      table_handle_define(handle),
      background_pool(db_context.getBackgroundPool()),
      settings(settings_),
      log(&Logger::get("DeltaMergeStore[" + db_name + "." + table_name + "]"))
{
    table_columns.emplace_back(table_handle_define);
    table_columns.emplace_back(getVersionColumnDefine());
    table_columns.emplace_back(getTagColumnDefine());

    for (auto & col : columns)
    {
        if (col.name != table_handle_define.name && col.name != VERSION_COLUMN_NAME && col.name != TAG_COLUMN_NAME)
            table_columns.emplace_back(col);
    }

    auto dm_context = newDMContext(db_context, db_context.getSettingsRef());
    if (!storage_pool.maxMetaPageId())
    {
        // Create the first segment.
        auto segment_id = storage_pool.newMetaPageId();
        if (segment_id != DELTA_MERGE_FIRST_SEGMENT_ID)
            throw Exception("The first segment id should be " + DB::toString(DELTA_MERGE_FIRST_SEGMENT_ID), ErrorCodes::LOGICAL_ERROR);
        auto first_segment = Segment::newSegment(*dm_context, HandleRange::newAll(), segment_id, 0);
        segments.emplace(first_segment->getRange().end, first_segment);
    }
    else
    {
        auto segment_id = DELTA_MERGE_FIRST_SEGMENT_ID;
        while (segment_id)
        {
            auto segment = Segment::restoreSegment(*dm_context, segment_id);
            segments.emplace(segment->getRange().end, segment);
            segment_id = segment->nextSegmentId();
        }
    }

    gc_handle              = background_pool.addTask([this] { return storage_pool.gc(); });
    background_task_handle = background_pool.addTask([this] { return handleBackgroundTask(); });
}

DeltaMergeStore::~DeltaMergeStore()
{
    background_pool.removeTask(gc_handle);
    background_pool.removeTask(background_task_handle);
}

inline Block getSubBlock(const Block & block, size_t offset, size_t limit)
{
    if (!offset && limit == block.rows())
    {
        return block;
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
        return sub_block;
    }
}

void DeltaMergeStore::write(const Context & db_context, const DB::Settings & db_settings, const Block & to_write)
{
    std::scoped_lock write_write_lock(write_write_mutex);

    EventRecorder write_block_recorder(ProfileEvents::DMWriteBlock, ProfileEvents::DMWriteBlockNS);

    const size_t rows = to_write.rows();
    if (rows == 0)
        return;

    auto  dm_context = newDMContext(db_context, db_settings);
    Block block      = to_write;

    // Add an extra handle column, if handle reused the original column data.
    if (pkIsHandle())
    {
        auto handle_pos = block.getPositionByName(table_handle_define.name);
        addColumnToBlock(block, //
                         EXTRA_HANDLE_COLUMN_ID,
                         EXTRA_HANDLE_COLUMN_NAME,
                         EXTRA_HANDLE_COLUMN_TYPE,
                         EXTRA_HANDLE_COLUMN_TYPE->createColumn());
        FunctionToInt64::create(db_context)->execute(block, {handle_pos}, block.columns() - 1);
    }

    {
        // Sort by handle & version in ascending order.
        SortDescription sort;
        sort.emplace_back(EXTRA_HANDLE_COLUMN_NAME, 1, 0);
        sort.emplace_back(VERSION_COLUMN_NAME, 1, 0);

        if (!isAlreadySorted(block, sort))
            stableSortBlock(block, sort);
    }

    if (log->trace())
    {
        std::shared_lock lock(read_write_mutex);

        String msg = "Before insert block(with " + DB::toString(rows) + " rows). All segments:{";
        for (auto & [end, segment] : segments)
        {
            (void)end;
            msg += DB::toString(segment->segmentId()) + ":" + segment->getRange().toString() + ",";
        }
        msg.pop_back();
        msg += "}";
        LOG_TRACE(log, msg);
    }

    // Locate which segments to write
    WriteActions actions = prepareWriteActions(block, segments, EXTRA_HANDLE_COLUMN_NAME, std::shared_lock(read_write_mutex));

    auto         op_context = OpContext::createForLogStorage(*dm_context);
    WriteBatches wbs;

    // Prepare updates' information.
    for (auto & action : actions)
    {
        action.update = getSubBlock(block, action.offset, action.limit);
        action.task   = action.segment->createAppendTask(op_context, wbs, action.update);
    }

    commitWrites(actions, wbs, dm_context, op_context);
}


void DeltaMergeStore::deleteRange(const Context & db_context, const DB::Settings & db_settings, const HandleRange & delete_range)
{
    std::scoped_lock write_write_lock(write_write_mutex);

    EventRecorder write_block_recorder(ProfileEvents::DMDeleteRange, ProfileEvents::DMDeleteRangeNS);

    if (delete_range.start >= delete_range.end)
        return;

    auto dm_context = newDMContext(db_context, db_settings);

    if (log->trace())
    {
        std::shared_lock lock(read_write_mutex);

        String msg = "Before delete range" + rangeToString(delete_range) + ". All segments:{";
        for (auto & [end, segment] : segments)
        {
            (void)end;
            msg += DB::toString(segment->segmentId()) + ":" + segment->getRange().toString() + ",";
        }
        msg.pop_back();
        msg += "}";
        LOG_TRACE(log, msg);
    }

    WriteActions actions = prepareWriteActions(delete_range, segments, std::shared_lock(read_write_mutex));

    auto         op_context = OpContext::createForLogStorage(*dm_context);
    WriteBatches wbs;

    // Prepare updates' information.
    for (auto & action : actions)
    {
        // action.update is set in `prepareWriteActions` for delete_range
        action.task = action.segment->createAppendTask(op_context, wbs, action.update);
    }

    // TODO: We need to do a delta merge after write a delete range, otherwise, the rows got deleted could never be actually removed.

    commitWrites(actions, wbs, dm_context, op_context);
}

void DeltaMergeStore::commitWrites(const WriteActions & actions,
                                   WriteBatches &       wbs,
                                   const DMContextPtr & dm_context,
                                   OpContext &          op_context)
{
    // Save generated chunks to disk.
    {
        EventRecorder recorder(ProfileEvents::DMAppendDeltaCommitDisk, ProfileEvents::DMAppendDeltaCommitDiskNS);
        wbs.writeLogAndData(dm_context->storage_pool);
    }

    Segments updated_segments;
    updated_segments.reserve(actions.size());

    {
        std::unique_lock lock(read_write_mutex);

        // Commit the meta of updates to memory and apply updates in memory.
        for (auto & action : actions)
        {
            // During write, segment instances could have been updated by background merge delta threads.
            // So we must get segment instances again from segments map.
            auto & range = action.segment->getRange();
            auto   it    = segments.find(range.end);
            if (unlikely(it == segments.end()))
                throw Exception("Segment with end [" + DB::toString(range.end) + "] can not find in segments map after write");
            auto & segment = it->second;
            updated_segments.push_back(segment);
            segment->applyAppendTask(op_context, action.task, action.update);
        }
    }

    // Clean up deleted data on disk.
    EventRecorder recorder(ProfileEvents::DMAppendDeltaCleanUp, ProfileEvents::DMAppendDeltaCleanUpNS);
    dm_context->storage_pool.log().write(wbs.removed_data);

    for (auto & segment : updated_segments)
    {
        checkSegmentUpdate<true>(dm_context, segment);
    }
}

BlockInputStreams DeltaMergeStore::readRaw(const Context &       db_context,
                                           const DB::Settings &  db_settings,
                                           const ColumnDefines & columns_to_read,
                                           size_t                num_streams)
{
    SegmentReadTasks   tasks;
    StorageSnapshotPtr storage_snapshot;

    auto dm_context = newDMContext(db_context, db_settings);

    {
        std::shared_lock lock(read_write_mutex);

        for (const auto & [handle, segment] : segments)
        {
            (void)handle;
            tasks.emplace_back(std::make_shared<SegmentReadTask>(segment, segment->getReadSnapshot(), HandleRanges{segment->getRange()}));
        }

        // The creation of storage snapshot must be put after creation of segment snapshot.
        storage_snapshot = std::make_shared<StorageSnapshot>(storage_pool);
    }

    auto stream_creator = [=](const SegmentReadTask & task) {
        return task.segment->getInputStreamRaw(*dm_context, columns_to_read, task.read_snapshot, *storage_snapshot);
    };
    auto after_segment_read = [=](const SegmentPtr & segment) { this->checkSegmentUpdate<false>(dm_context, segment); };

    size_t final_num_stream = std::min(num_streams, tasks.size());
    auto   read_task_pool   = std::make_shared<SegmentReadTaskPool>(std::move(tasks));

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>( //
            read_task_pool,
            stream_creator,
            after_segment_read,
            columns_to_read);
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
    SegmentReadTasks   tasks;
    StorageSnapshotPtr storage_snapshot;

    auto dm_context = newDMContext(db_context, db_settings);

    {
        std::shared_lock lock(read_write_mutex);

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
                if (tasks.empty() || tasks.back()->segment != seg_it->second)
                {
                    auto segment = seg_it->second;
                    tasks.emplace_back(std::make_shared<SegmentReadTask>(segment, segment->getReadSnapshot()));
                }

                tasks.back()->addRange(req_range);

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

        // The creation of storage snapshot must be put after creation of segment snapshot.
        storage_snapshot = std::make_shared<StorageSnapshot>(storage_pool);
    }

    auto stream_creator = [=](const SegmentReadTask & task) {
        return task.segment->getInputStream(*dm_context,
                                            columns_to_read,
                                            task.read_snapshot,
                                            *storage_snapshot,
                                            task.ranges,
                                            {},
                                            max_version,
                                            std::max(expected_block_size, STABLE_CHUNK_ROWS));
    };
    auto after_segment_read = [this, dm_context](const SegmentPtr & segment) { this->checkSegmentUpdate<false>(dm_context, segment); };

    size_t final_num_stream = std::min(num_streams, tasks.size());
    auto   read_task_pool   = std::make_shared<SegmentReadTaskPool>(std::move(tasks));

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>( //
            read_task_pool,
            stream_creator,
            after_segment_read,
            columns_to_read);
        res.push_back(stream);
    }
    return res;
}

template <bool by_write_thread>
void DeltaMergeStore::checkSegmentUpdate(const DMContextPtr & dm_context, const SegmentPtr & segment)
{
    if constexpr (!by_write_thread)
    {
        if (segment->isMergeDelta())
            return;
    }

    size_t delta_rows    = segment->deltaRows(/* with_delta_cache */ by_write_thread);
    size_t delta_updates = segment->updatesInDeltaTree();

    bool should_background_merge_delta = std::max(delta_rows, delta_updates) >= dm_context->delta_limit_rows;
    bool should_foreground_merge_delta = std::max(delta_rows, delta_updates) >= dm_context->segment_limit_rows * 5;

    if (by_write_thread)
    {
        // Only write thread will check split & merge.
        size_t segment_rows = segment->getEstimatedRows();

        bool should_split = segment_rows >= dm_context->segment_limit_rows * 2;
        bool force_split  = segment_rows >= dm_context->segment_limit_rows * 10;

        bool should_merge = segment_rows < dm_context->segment_limit_rows / 4;

        if (force_split || (should_split && !segment->isBackgroundMergeDelta()))
        {
            auto [left, right] = segmentSplit(*dm_context, segment);
            if (left)
            {
                checkSegmentUpdate<by_write_thread>(dm_context, left);
                checkSegmentUpdate<by_write_thread>(dm_context, right);
            }
            return;
        }

        if (should_foreground_merge_delta)
        {
            segmentForegroundMergeDelta(*dm_context, segment);
            return;
        }

        if (should_background_merge_delta)
        {
            bool v = false;
            if (!segment->isBackgroundMergeDelta().compare_exchange_strong(v, true))
                return;

            merge_delta_tasks.addTask({dm_context, segment, BackgroundType::MergeDelta}, "write thread", log);
            background_task_handle->wake();
            return;
        }

        if (should_merge)
        {
            // The last segment.
            if (segment->getRange().end == P_INF_HANDLE)
                return;
            if (segment->isBackgroundMergeDelta() || segment->isMergeDelta())
                return;
            segmentForegroundMerge(*dm_context, segment);
            return;
        }
    }
    else
    {
        // Other threads only do background merge delta.
        if (should_foreground_merge_delta || should_background_merge_delta)
        {
            bool v = false;
            if (!segment->isBackgroundMergeDelta().compare_exchange_strong(v, true))
                return;

            merge_delta_tasks.addTask({dm_context, segment, BackgroundType::MergeDelta}, "other thread", log);
            background_task_handle->wake();
        }
        return;
    }
}

SegmentPair DeltaMergeStore::segmentSplit(DMContext & dm_context, const SegmentPtr & segment)
{
    LOG_DEBUG(log, "Split segment " << segment->info() << " #rows: " << segment->getEstimatedRows());

    SegmentSnapshot    segment_snap;
    StorageSnapshotPtr storage_snap;

    {
        std::shared_lock lock(read_write_mutex);

        segment_snap = segment->getReadSnapshot(/* use_delta_cache */ true);
        storage_snap = std::make_shared<StorageSnapshot>(storage_pool);
    }

    WriteBatches wbs;
    auto         range                 = segment->getRange();
    auto [new_seg_left, new_seg_right] = segment->split(dm_context, segment_snap, *storage_snap, wbs);
    wbs.writeLogAndData(storage_pool);

    {
        std::unique_lock lock(read_write_mutex);

        if (!isSegmentValid(segment))
        {
            LOG_DEBUG(log, "Give up segment [" << segment->segmentId() << "] split");

            wbs.rollbackWrittenLogAndData(storage_pool);
            return {};
        }

        LOG_DEBUG(log, "Segment [" << segment->segmentId() << "] apply split");

        wbs.writeMeta(storage_pool);

        segments.erase(range.end);
        segments[new_seg_left->getRange().end]  = new_seg_left;
        segments[new_seg_right->getRange().end] = new_seg_right;

        if constexpr (DM_RUN_CHECK)
        {
            new_seg_left->check(dm_context, "After split left");
            new_seg_right->check(dm_context, "After split right");
        }
    }

    wbs.writeRemoves(storage_pool);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);

    return {new_seg_left, new_seg_right};
}

void DeltaMergeStore::segmentMerge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right)
{
    LOG_DEBUG(log, "Merge segment, left:" << left->info() << ", right:" << right->info());

    SegmentSnapshot    left_snap;
    SegmentSnapshot    right_snap;
    StorageSnapshotPtr storage_snap;

    {
        std::shared_lock lock(read_write_mutex);

        left_snap    = left->getReadSnapshot(/* use_delta_cache */ true);
        right_snap   = right->getReadSnapshot(/* use_delta_cache */ true);
        storage_snap = std::make_shared<StorageSnapshot>(storage_pool);
    }

    auto left_range  = left->getRange();
    auto right_range = right->getRange();

    WriteBatches wbs;
    auto         merged = Segment::merge(dm_context, left, left_snap, right, right_snap, *storage_snap, wbs);
    wbs.writeLogAndData(storage_pool);

    {
        std::unique_lock lock(read_write_mutex);

        if (!isSegmentValid(left) || !isSegmentValid(right))
        {
            LOG_DEBUG(log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");

            wbs.rollbackWrittenLogAndData(storage_pool);
            return;
        }

        LOG_DEBUG(log, "Segments left [" << left->segmentId() << "], right [" << right->segmentId() << "] apply merge");

        wbs.writeMeta(storage_pool);

        segments.erase(left_range.end);
        segments.erase(right_range.end);
        segments.emplace(merged->getRange().end, merged);

        if constexpr (DM_RUN_CHECK)
        {
            merged->check(dm_context, "After segment merge");
        }
    }

    wbs.writeRemoves(storage_pool);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);
}

void DeltaMergeStore::segmentMergeDelta(DMContext &             dm_context,
                                        const SegmentPtr &      segment,
                                        const SegmentSnapshot & segment_snap,
                                        const StorageSnapshot & storage_snap,
                                        bool                    is_foreground)
{
    bool v = false;
    if (!segment->isMergeDelta().compare_exchange_strong(v, true))
        return;

    LOG_DEBUG(log, "Segment [" << segment->segmentId() << "] start " << (is_foreground ? "foreground" : "background") << " merge delta");

    WriteBatches wbs;
    auto         new_stable = segment->prepareMergeDelta(dm_context, segment_snap, storage_snap, wbs);
    wbs.writeLogAndData(storage_pool);

    {
        std::unique_lock read_write_lock(read_write_mutex);

        if (!is_foreground && (!isSegmentValid(segment)))
        {
            // Give up this merge delta operation if
            //  1. current segment is invalid. It is caused by other threads do merge/split/merge dela concurrently.
            //  2. This segment is doing foreground merge delta already.

            // Note that foreground merge delta won't fail.

            LOG_DEBUG(log, "Segment [" << segment->segmentId() << "] give up background merge delta");

            wbs.rollbackWrittenLogAndData(storage_pool);
            return;
        }

        LOG_DEBUG(log,
                  "Segment [" << segment->segmentId() << "] apply " << (is_foreground ? "foreground" : "background") << " merge delta");

        auto new_segment = segment->applyMergeDelta(segment_snap, wbs, new_stable);

        wbs.writeMeta(storage_pool);

        segments[segment->getRange().end] = new_segment;

        if constexpr (DM_RUN_CHECK)
        {
            new_segment->check(dm_context, "After merge delta");
        }
    }

    wbs.writeRemoves(storage_pool);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);
}

void DeltaMergeStore::segmentForegroundMergeDelta(DMContext & dm_context, const SegmentPtr & segment)
{
    LOG_DEBUG(log, "Segment [" << segment->segmentId() << "] foreground merge delta");

    SegmentSnapshot    segment_snap;
    StorageSnapshotPtr storage_snap;

    {
        std::shared_lock lock(read_write_mutex);

        segment_snap = segment->getReadSnapshot(/* use_delta_cache */ true);
        storage_snap = std::make_shared<StorageSnapshot>(storage_pool);
    }

    segmentMergeDelta(dm_context, segment, segment_snap, *storage_snap, true);
}

void DeltaMergeStore::segmentBackgroundMergeDelta(DMContext & dm_context, const SegmentPtr & segment)
{
    SegmentSnapshot    segment_snap;
    StorageSnapshotPtr storage_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(segment))
            return;

        segment_snap = segment->getReadSnapshot(/* use_delta_cache */ false);
        storage_snap = std::make_shared<StorageSnapshot>(storage_pool);
    }

    LOG_DEBUG(log, "Segment [" << segment->segmentId() << "] background merge delta");

    segmentMergeDelta(dm_context, segment, segment_snap, *storage_snap, false);
}

void DeltaMergeStore::segmentForegroundMerge(DMContext & dm_context, const SegmentPtr & segment)
{
    if (segment->isMergeDelta())
        return;

    SegmentPtr next_segment;
    {
        std::shared_lock read_write_lock(read_write_mutex);

        auto it = segments.find(segment->getRange().end);
        // check legality
        if (it == segments.end())
            return;
        auto & cur_segment = it->second;
        if (cur_segment.get() != segment.get())
            return;
        ++it;
        if (it == segments.end())
            return;
        next_segment = it->second;

        auto limit = dm_context.segment_limit_rows / 4;
        if (segment->getEstimatedRows() >= limit || next_segment->getEstimatedRows() >= limit)
            return;
    }

    segmentMerge(dm_context, segment, next_segment);
}

bool DeltaMergeStore::handleBackgroundTask()
{
    auto task = merge_delta_tasks.nextTask(log);
    if (!task)
        return false;

    switch (task.type)
    {
    case MergeDelta:
        segmentBackgroundMergeDelta(*task.dm_context, task.segment);
        break;
    default:
        throw Exception("Unsupport task type: " + getBackgroundTypeName(task.type));
    }

    return true;
}

bool DeltaMergeStore::isSegmentValid(const SegmentPtr & segment)
{
    // Segment instance could have been removed or replaced.
    auto it = segments.find(segment->getRange().end);
    if (it == segments.end())
        return false;
    auto & cur_segment = it->second;
    return cur_segment.get() == segment.get();
}

void DeltaMergeStore::flushCache(const Context & db_context)
{
    std::scoped_lock lock(write_write_mutex);

    auto dm_context = newDMContext(db_context, db_context.getSettingsRef());
    for (auto && [_handle, segment] : segments)
    {
        (void)_handle;
        segment->flushCache(*dm_context);
    }
}

void DeltaMergeStore::check(const Context & /*db_context*/)
{
    std::shared_lock lock(read_write_mutex);

    UInt64 next_segment_id = DELTA_MERGE_FIRST_SEGMENT_ID;
    Handle last_end        = N_INF_HANDLE;
    for (const auto & [end, segment] : segments)
    {
        auto segment_id = segment->segmentId();
        auto range      = segment->getRange();

        if (next_segment_id != segment_id)
        {
            String msg = "Check failed. Segments: ";
            for (auto & [end, segment] : segments)
            {
                (void)end;
                msg += DB::toString(end) + "->" + segment->info() + ",";
            }
            msg.pop_back();
            msg += "}";
            LOG_ERROR(log, msg);

            throw Exception("Segment [" + DB::toString(segment_id) + "] is expected to have id [" + DB::toString(next_segment_id) + "]");
        }
        if (last_end != range.start)
            throw Exception("Segment [" + DB::toString(segment_id) + "] range start[" + DB::toString(range.start)
                            + "] is not equal to last_end[" + DB::toString(last_end) + "]");

        last_end        = end;
        next_segment_id = segment->nextSegmentId();
    }
    if (last_end != P_INF_HANDLE)
        throw Exception("Last segment range end[" + DB::toString(last_end) + "] is not equal to P_INF_HANDLE");
}

DeltaMergeStoreStat DeltaMergeStore::getStat()
{
    std::shared_lock lock(read_write_mutex);

    DeltaMergeStoreStat stat;

    stat.segment_count = segments.size();

    long total_placed_rows = 0;

    for (const auto & [handle, segment] : segments)
    {
        (void)handle;
        auto delta  = segment->getDelta();
        auto stable = segment->getStable();

        stat.total_rows += delta.num_rows() + stable.num_rows();
        stat.total_bytes += delta.num_bytes() + stable.num_bytes();

        total_placed_rows += segment->getPlacedDeltaRows();

        if (delta.num_chunks())
        {
            stat.total_delete_ranges += delta.num_deletes();

            stat.segment_count_with_delta += 1;
            stat.delta_count += 1;
            stat.total_chunk_count_in_delta += delta.num_chunks();

            stat.total_delta_rows += delta.num_rows();
            stat.total_delta_bytes += delta.num_bytes();
        }

        if (stable.num_chunks())
        {
            stat.segment_count_with_stable += 1;
            stat.stable_count += 1;
            stat.total_chunk_count_in_stable += stable.num_chunks();

            stat.total_stable_rows += stable.num_rows();
            stat.total_stable_bytes += stable.num_bytes();
        }
    }

    stat.delta_placed_rate = (Float64)total_placed_rows / stat.total_delta_rows;

    stat.avg_segment_rows  = (Float64)stat.total_rows / stat.segment_count;
    stat.avg_segment_bytes = (Float64)stat.total_bytes / stat.segment_count;

    stat.avg_delta_rows          = (Float64)stat.total_delta_rows / stat.delta_count;
    stat.avg_delta_bytes         = (Float64)stat.total_delta_bytes / stat.delta_count;
    stat.avg_delta_delete_ranges = (Float64)stat.total_delete_ranges / stat.delta_count;

    stat.avg_stable_rows  = (Float64)stat.total_stable_rows / stat.stable_count;
    stat.avg_stable_bytes = (Float64)stat.total_stable_bytes / stat.stable_count;

    stat.avg_chunk_count_in_delta = (Float64)stat.total_chunk_count_in_delta / stat.delta_count;
    stat.avg_chunk_rows_in_delta  = (Float64)stat.total_delta_rows / stat.total_chunk_count_in_delta;
    stat.avg_chunk_bytes_in_delta = (Float64)stat.total_delta_bytes / stat.total_chunk_count_in_delta;

    stat.avg_chunk_count_in_stable = (Float64)stat.total_chunk_count_in_stable / stat.stable_count;
    stat.avg_chunk_rows_in_stable  = (Float64)stat.total_stable_rows / stat.total_chunk_count_in_stable;
    stat.avg_chunk_bytes_in_stable = (Float64)stat.total_stable_bytes / stat.total_chunk_count_in_stable;

    return stat;
}

void DeltaMergeStore::applyAlters(const AlterCommands &         commands,
                                  const OptionTableInfoConstRef table_info,
                                  ColumnID &                    max_column_id_used,
                                  const Context &               context)
{
    /// Force flush on store, so that no chunks with different data type in memory
    // TODO maybe some ddl do not need to flush cache? eg. just change default value
    this->flushCache(context);

    for (const auto & command : commands)
    {
        applyAlter(command, table_info, max_column_id_used);
    }
}

namespace
{
// TODO maybe move to -internal.h ?
inline void setColumnDefineDefaultValue(const AlterCommand & command, ColumnDefine & define)
{
    if (command.default_expression)
    {
        // a cast function
        // change column_define.default_value

        if (auto default_literal = typeid_cast<const ASTLiteral *>(command.default_expression.get());
            default_literal && default_literal->value.getType() == Field::Types::String)
        {
            define.default_value = default_literal->value;
        }
        else if (auto default_cast_expr = typeid_cast<const ASTFunction *>(command.default_expression.get());
                 default_cast_expr && default_cast_expr->name == "CAST" /* ParserCastExpression::name */)
        {
            // eg. CAST('1.234' AS Float32); CAST(999 AS Int32)
            if (default_cast_expr->arguments->children.size() != 2)
            {
                throw Exception("Unknown CAST expression in default expr", ErrorCodes::NOT_IMPLEMENTED);
            }

            auto default_literal_in_cast = typeid_cast<const ASTLiteral *>(default_cast_expr->arguments->children[0].get());
            if (default_literal_in_cast)
            {
                define.default_value = default_literal_in_cast->value;
            }
            else
            {
                throw Exception("Invalid CAST expression", ErrorCodes::BAD_ARGUMENTS);
            }
        }
        else
        {
            throw Exception("Default value must be a string or CAST('...' AS WhatType)", ErrorCodes::BAD_ARGUMENTS);
        }
    }
}
} // namespace

void DeltaMergeStore::applyAlter(const AlterCommand & command, const OptionTableInfoConstRef table_info, ColumnID & max_column_id_used)
{
    /// Caller should ensure the command is legal.
    /// eg. The column to modify/drop/rename must exist, the column to add must not exist, the new column name of rename must not exists.

    if (command.type == AlterCommand::MODIFY_COLUMN)
    {
        // find column define and then apply modify
        bool exist_column = false;
        for (auto && column_define : table_columns)
        {
            if (column_define.id == command.column_id)
            {
                exist_column       = true;
                column_define.type = command.data_type;
                setColumnDefineDefaultValue(command, column_define);
                break;
            }
        }
        if (unlikely(!exist_column))
        {
            // Fall back to find column by name, this path should only call by tests.
            LOG_WARNING(log,
                        "Try to apply alter to column: " << command.column_name << ", id:" << toString(command.column_id)
                                                         << ", but not found by id, fall back locating col by name.");
            for (auto && column_define : table_columns)
            {
                if (column_define.name == command.column_name)
                {
                    exist_column       = true;
                    column_define.type = command.data_type;
                    setColumnDefineDefaultValue(command, column_define);
                    break;
                }
            }
            if (unlikely(!exist_column))
            {
                throw Exception(String("Alter column: ") + command.column_name + " is not exists.", ErrorCodes::LOGICAL_ERROR);
            }
        }
    }
    else if (command.type == AlterCommand::ADD_COLUMN)
    {
        // we don't care about `after_column` in `store_columns`

        /// If TableInfo from TiDB is not empty, we get column id from TiDB
        /// else we allocate a new id by `max_column_id_used`
        ColumnDefine define(0, command.column_name, command.data_type);
        if (table_info)
        {
            define.id = table_info->get().getColumnID(command.column_name);
        }
        else
        {
            define.id = max_column_id_used++;
        }
        setColumnDefineDefaultValue(command, define);
        table_columns.emplace_back(std::move(define));
    }
    else if (command.type == AlterCommand::DROP_COLUMN)
    {
        table_columns.erase(
            std::remove_if(table_columns.begin(), table_columns.end(), [&](const ColumnDefine & c) { return c.id == command.column_id; }),
            table_columns.end());
    }
    else if (command.type == AlterCommand::RENAME_COLUMN)
    {
        for (auto && c : table_columns)
        {
            if (c.id == command.column_id)
            {
                c.name = command.new_column_name;
                break;
            }
        }
    }
    else
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << " receive unknown alter command, type: " << toString(static_cast<Int32>(command.type)));
    }
}

SortDescription DeltaMergeStore::getPrimarySortDescription() const
{
    SortDescription desc;
    desc.emplace_back(table_handle_define.name, /* direction_= */ 1, /* nulls_direction_= */ 1);
    return desc;
}

} // namespace DM
} // namespace DB

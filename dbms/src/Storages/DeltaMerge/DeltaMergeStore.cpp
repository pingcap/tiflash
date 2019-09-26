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

    gc_handle = background_pool.addTask([this] { return storage_pool.gc(); });
}

DeltaMergeStore::~DeltaMergeStore()
{
    background_pool.removeTask(gc_handle);
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
    std::unique_lock write_write_lock(write_write_mutex);

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

    auto               op_context = OpContext::createForLogStorage(*dm_context);
    AppendWriteBatches wbs;

    // Prepare updates' information.
    for (auto & action : actions)
    {
        action.update = getSubBlock(block, action.offset, action.limit);
        action.task   = action.segment->createAppendTask(op_context, wbs, action.update);
    }

    commitWrites(std::move(actions), std::move(wbs), *dm_context, op_context, db_context, db_settings, true);
}


void DeltaMergeStore::deleteRange(const Context & db_context, const DB::Settings & db_settings, const HandleRange & delete_range)
{
    std::unique_lock write_write_lock(write_write_mutex);

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

    auto               op_context = OpContext::createForLogStorage(*dm_context);
    AppendWriteBatches wbs;

    // Prepare updates' information.
    for (auto & action : actions)
    {
        // action.update is set in `prepareWriteActions` for delete_range
        action.task = action.segment->createAppendTask(op_context, wbs, action.update);
    }

    // TODO: We need to do a delta merge after write a delete range, otherwise, the rows got deleted could never be acutally removed.

    commitWrites(std::move(actions), std::move(wbs), *dm_context, op_context, db_context, db_settings, false);
}

void DeltaMergeStore::commitWrites(WriteActions &&       actions,
                                   AppendWriteBatches && wbs,
                                   DMContext &           dm_context,
                                   OpContext &           op_context,
                                   const Context &       db_context,
                                   const DB::Settings &  db_settings,
                                   bool                  is_upsert)
{
    // Commit updates to disk.
    {
        EventRecorder recorder(ProfileEvents::DMAppendDeltaCommitDisk, ProfileEvents::DMAppendDeltaCommitDiskNS);
        dm_context.storage_pool.log().write(wbs.data);
        dm_context.storage_pool.meta().write(wbs.meta);
    }

    // Commit updates in memory.
    for (auto & action : actions)
    {
        action.segment->applyAppendTask(op_context, action.task, action.update);
    }

    // Clean up deleted data on disk.
    {
        std::unique_lock lock(read_write_mutex);

        EventRecorder recorder(ProfileEvents::DMAppendDeltaCleanUp, ProfileEvents::DMAppendDeltaCleanUpNS);
        dm_context.storage_pool.log().write(wbs.removed_data);
    }

    RemoveWriteBatches remove_wbs;

    // Split or merge delta if needed.

    // TODO: maybe we should do checking only after we have accumulated enough updates.

    size_t               segment_rows_setting = db_settings.dm_segment_rows;
    std::set<SegmentPtr> to_delta_merge_segments;

    for (auto & action : actions)
    {
        const auto & segment = action.segment;
        const auto   range   = segment->getRange();

        if (shouldSplit(segment, segment_rows_setting))
        {
            auto [new_seg_left, new_seg_right] = segment->split(dm_context, remove_wbs);

            {
                std::unique_lock lock(read_write_mutex);

                segments.erase(range.end);
                segments[new_seg_left->getRange().end]  = new_seg_left;
                segments[new_seg_right->getRange().end] = new_seg_right;
            }

            if (new_seg_left->shouldFlushDelta(dm_context))
                to_delta_merge_segments.insert(new_seg_left);
            if (new_seg_right->shouldFlushDelta(dm_context))
                to_delta_merge_segments.insert(new_seg_right);

#ifndef NDEBUG
            new_seg_left->check(dm_context, "After split(left). " + new_seg_left->simpleInfo());
            new_seg_right->check(dm_context, "After split(right). " + new_seg_right->simpleInfo());
#endif
        }
        else
        {
            if (segment->shouldFlushDelta(dm_context))
                to_delta_merge_segments.insert(segment);
        }
    }

    for (auto & segment : to_delta_merge_segments)
    {
        const auto & range       = segment->getRange();
        const auto   new_segment = segment->flushDelta(dm_context, remove_wbs);
        {
            std::unique_lock lock(read_write_mutex);

            segments[range.end] = new_segment;
        }
    }

    remove_wbs.write(dm_context.storage_pool);

    // Merge if needed.

    // TODO: we should only check those segments which has got delete range written into.

    if (!is_upsert)
        afterDelete(db_context, db_settings);
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
        return task.segment->getInputStreamRaw(*dm_context, task.read_snapshot, *storage_snapshot, columns_to_read);
    };

    size_t final_num_stream = std::min(num_streams, tasks.size());
    auto   read_task_pool   = std::make_shared<SegmentReadTaskPool>(std::move(tasks));

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>(read_task_pool, stream_creator, columns_to_read);
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
                                            task.read_snapshot,
                                            *storage_snapshot,
                                            columns_to_read,
                                            task.ranges,
                                            {},
                                            max_version,
                                            std::min(expected_block_size, DEFAULT_BLOCK_SIZE));
    };

    size_t final_num_stream = std::min(num_streams, tasks.size());
    auto   read_task_pool   = std::make_shared<SegmentReadTaskPool>(std::move(tasks));

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>(read_task_pool, stream_creator, columns_to_read);
        res.push_back(stream);
    }
    return res;
}

bool DeltaMergeStore::afterDelete(const Context & db_context, const DB::Settings & db_settings)
{
    auto   dm_context           = newDMContext(db_context, db_settings);
    size_t segment_rows_setting = db_settings.dm_segment_rows;

    while (true)
    {
        /// TODO: fix this naive algorithm, to reduce merge/split frequency.

        SegmentPtr current;
        SegmentPtr next;

        {
            std::shared_lock lock(read_write_mutex);

            auto it = segments.begin();
            while (it != segments.end())
            {
                auto & segment = it->second;
                ++it;
                if (it == segments.end())
                    break;
                auto & next_segment = it->second;
                if (shouldMerge(segment, next_segment, segment_rows_setting))
                {
                    current = segment;
                    next    = next_segment;
                    break;
                }
            }
        }

        if (current)
        {
            merge(*dm_context, current, next);
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
    LOG_DEBUG(log, "Split segment " << segment->info() << " #rows: " << segment->getEstimatedRows());

    RemoveWriteBatches remove_wbs;
    auto               range           = segment->getRange();
    auto [new_seg_left, new_seg_right] = segment->split(dm_context, remove_wbs);

    {
        std::unique_lock lock(read_write_mutex);

        segments.erase(range.end);
        segments[new_seg_left->getRange().end]  = new_seg_left;
        segments[new_seg_right->getRange().end] = new_seg_right;

        remove_wbs.write(dm_context.storage_pool);
    }

#ifndef NDEBUG
//    new_seg_left->check(dm_context, "After split(left). " + new_seg_left->simpleInfo());
//    new_seg_right->check(dm_context, "After split(right). " + new_seg_right->simpleInfo());
#endif
}

void DeltaMergeStore::merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right)
{
    LOG_DEBUG(log, "Merge segment, left:" << left->info() << ", right:" << right->info());

    auto left_range  = left->getRange();
    auto right_range = right->getRange();

    RemoveWriteBatches remove_wbs;
    auto               merged = Segment::merge(dm_context, left, right, remove_wbs);

    {
        std::unique_lock lock(read_write_mutex);

        segments.erase(left_range.end);
        segments.erase(right_range.end);
        segments.emplace(merged->getRange().end, merged);

        remove_wbs.write(dm_context.storage_pool);
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
        segment->check(*dm_context, "Manually");
    }
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
            const auto default_val = safeGet<String>(default_literal->value);
            define.default_value   = default_val;
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
            if (default_literal_in_cast && default_literal_in_cast->value.getType() == Field::Types::String)
            {
                const auto default_value = safeGet<String>(default_literal_in_cast->value);
                define.default_value     = default_value;
            }
            else
            {
                throw Exception("First argument in CAST expression must be a string", ErrorCodes::NOT_IMPLEMENTED);
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

void DeltaMergeStore::flushCache(const Context & db_context)
{
    auto dm_context = newDMContext(db_context, db_context.getSettingsRef());
    for (auto && [_handle, segment] : segments)
    {
        (void)_handle;
        WriteBatch remove_log_wb;
        segment->flushCache(*dm_context, remove_log_wb);

        {
            std::unique_lock lock(read_write_mutex);
            dm_context->storage_pool.log().write(remove_log_wb);
        }
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

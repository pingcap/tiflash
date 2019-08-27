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
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace ProfileEvents
{
extern const Event DMWriteBlock;
extern const Event DMWriteBlockNS;
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
    // update block header
    header = genHeaderBlock(table_columns, table_handle_define, table_handle_real_type);

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
    EventRecorder write_block_recorder(ProfileEvents::DMWriteBlock, ProfileEvents::DMWriteBlockNS);

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

        BlockOrDelete update = {};
        AppendTaskPtr task   = {};
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

            actions.emplace_back(WriteAction{.segment = segment, .offset = offset, .limit = limit});

            offset += limit;
        }
    }

    auto               op_context = OpContext::createForLogStorage(dm_context);
    AppendWriteBatches wbs;

    // Prepare updates' information.
    for (auto & action : actions)
    {
        action.update = getSubBlock(block, action.offset, action.limit);
        action.task   = action.segment->createAppendTask(op_context, wbs, action.update);
    }

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

    // Flush delta if needed.
    for (auto & action : actions)
    {
        const auto & segment = action.segment;
        const auto   range   = segment->getRange();
        // TODO: Do flush by background threads.
        if (segment->shouldFlush(dm_context))
        {
            auto new_segment = action.segment->flush(dm_context);
            {
                std::shared_lock lock(mutex);
                segments[range.end] = new_segment;
            }
        }
    }

    // Clean up deleted data on disk.
    {
        EventRecorder recorder(ProfileEvents::DMAppendDeltaCleanUp, ProfileEvents::DMAppendDeltaCleanUpNS);
        dm_context.storage_pool.log().write(wbs.removed_data);
    }

    // TODO: Should only check the updated segments.
    afterInsertOrDelete(db_context, db_settings);
}

BlockInputStreams DeltaMergeStore::readRaw(const Context &       db_context,
                                           const DB::Settings &  db_settings,
                                           const ColumnDefines & columns_to_read,
                                           size_t                num_streams)
{
    SegmentReadTasks   tasks;
    StorageSnapshotPtr storage_snapshot;

    {
        std::shared_lock lock(mutex);

        storage_snapshot = std::make_shared<StorageSnapshot>(storage_pool);

        for (const auto & [handle, segment] : segments)
        {
            (void)handle;
            tasks.emplace_back(std::make_shared<SegmentReadTask>(segment, segment->getReadSnapshot(), HandleRanges{segment->getRange()}));
        }
    }

    auto dm_context = newDMContext(db_context, db_settings);

    auto stream_creator = [=](const SegmentReadTask & task) {
        return task.segment->getInputStreamRaw(dm_context, task.read_snapshot, *storage_snapshot, columns_to_read);
    };

    size_t final_num_stream = std::min(num_streams, tasks.size());
    auto   read_task_pool   = std::make_shared<SegmentReadTaskPool>(std::move(tasks));

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>( //
            read_task_pool,
            stream_creator,
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
    SegmentReadTasks   tasks;
    StorageSnapshotPtr storage_snapshot;

    {
        std::shared_lock lock(mutex);

        /// FIXME: the creation of storage_snapshot is not atomic!
        storage_snapshot = std::make_shared<StorageSnapshot>(storage_pool);

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
    }

    auto dm_context = newDMContext(db_context, db_settings);

    SegmentStreamCreator stream_creator = [=](const SegmentReadTask & task) {
        return task.segment->getInputStream(dm_context,
                                            task.read_snapshot,
                                            *storage_snapshot,
                                            columns_to_read,
                                            task.ranges,
                                            max_version,
                                            std::min(expected_block_size, DEFAULT_BLOCK_SIZE));
    };

    size_t final_num_stream = std::min(num_streams, tasks.size());
    auto   read_task_pool   = std::make_shared<SegmentReadTaskPool>(std::move(tasks));

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>( //
            read_task_pool,
            stream_creator,
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
        bool       is_split = false;

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

Block DeltaMergeStore::genHeaderBlock(const ColumnDefines & raw_columns,
                                      const ColumnDefine &  handle_define,
                                      const DataTypePtr &   handle_real_type)
{
    ColumnDefines real_cols = raw_columns;
    for (auto && col : real_cols)
    {
        if (col.id == handle_define.id)
        {
            if (handle_real_type)
                col.type = handle_real_type;
        }
    }
    return toEmptyBlock(real_cols);
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

    // Don't forget to update header
    header = genHeaderBlock(table_columns, table_handle_define, table_handle_real_type);
}

namespace
{
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
    if (command.type == AlterCommand::MODIFY_COLUMN)
    {
        // find column define and then apply modify
        bool exist_column = false;
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
        if (!exist_column)
        {
            throw Exception(String("Alter column: ") + command.column_name + " is not exists.", ErrorCodes::LOGICAL_ERROR);
        }
    }
    else if (command.type == AlterCommand::ADD_COLUMN)
    {
        // we don't care about `after_column` in `table_columns`

        /// If TableInfo from TiDB is not empty, we get column id from TiDB
        ColumnDefine define(0, command.column_name, command.data_type);
        if (table_info)
        {
            auto tidb_col_iter = findColumnInfoInTableInfo(table_info->get(), command.column_name);
            define.id          = tidb_col_iter->id;
        }
        else
        {
            define.id = max_column_id_used++;
        }
        assert(define.id != 0);
        setColumnDefineDefaultValue(command, define);
        table_columns.emplace_back(std::move(define));
    }
    else if (command.type == AlterCommand::DROP_COLUMN)
    {
        // identify column by name in `AlterCommand`. TODO we may change to identify column by column-id later
        table_columns.erase(std::remove_if(table_columns.begin(),
                                           table_columns.end(),
                                           [&](const ColumnDefine & c) { return c.name == command.column_name; }),
                            table_columns.end());
    }
}

void DeltaMergeStore::flushCache(const Context & db_context)
{
    DMContext dm_context = newDMContext(db_context, db_context.getSettingsRef());
    for (auto && [_handle, segment] : segments)
    {
        std::unique_lock lock(mutex);
        (void)_handle;
        segment->flushCache(dm_context);
    }
}

} // namespace DM
} // namespace DB
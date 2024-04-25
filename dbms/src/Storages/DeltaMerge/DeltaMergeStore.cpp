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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/FmtUtils.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/assert_cast.h>
#include <Core/SortDescription.h>
#include <Functions/FunctionsConversion.h>
#include <Interpreters/sortBlock.h>
#include <Poco/Exception.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMSegmentThreadInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/ReadThread/UnorderedInputStream.h>
#include <Storages/DeltaMerge/SchemaUpdate.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/VersionSet/PageEntriesVersionSetWithDelta.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

#include <atomic>
#include <ext/scope_guard.h>
#include <magic_enum.hpp>
#include <memory>

namespace ProfileEvents
{
extern const Event DMWriteBlock;
extern const Event DMWriteBlockNS;
extern const Event DMWriteFile;
extern const Event DMWriteFileNS;
extern const Event DMDeleteRange;
extern const Event DMDeleteRangeNS;
extern const Event DMAppendDeltaCommitDisk;
extern const Event DMAppendDeltaCommitDiskNS;
extern const Event DMAppendDeltaCleanUp;
extern const Event DMAppendDeltaCleanUpNS;

} // namespace ProfileEvents

namespace CurrentMetrics
{
extern const Metric DT_DeltaMergeTotalBytes;
extern const Metric DT_DeltaMergeTotalRows;
extern const Metric DT_SnapshotOfRead;
extern const Metric DT_SnapshotOfReadRaw;
extern const Metric DT_SnapshotOfPlaceIndex;
} // namespace CurrentMetrics

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char skip_check_segment_update[];
extern const char pause_when_writing_to_dt_store[];
extern const char pause_when_altering_dt_store[];
extern const char force_triggle_background_merge_delta[];
extern const char force_triggle_foreground_flush[];
extern const char random_exception_after_dt_write_done[];
extern const char force_slow_page_storage_snapshot_release[];
extern const char exception_before_drop_segment[];
extern const char exception_after_drop_segment[];
} // namespace FailPoints

namespace DM
{
// ================================================
//   MergeDeltaTaskPool
// ================================================

std::pair<bool, bool> DeltaMergeStore::MergeDeltaTaskPool::tryAddTask(const BackgroundTask & task, const ThreadType & whom, const size_t max_task_num, const LoggerPtr & log_)
{
    std::scoped_lock lock(mutex);
    if (light_tasks.size() + heavy_tasks.size() >= max_task_num)
        return std::make_pair(false, false);

    bool is_heavy = false;
    switch (task.type)
    {
    case TaskType::Split:
    case TaskType::MergeDelta:
        is_heavy = true;
        // reserve some task space for light tasks
        if (max_task_num > 1 && heavy_tasks.size() >= static_cast<size_t>(max_task_num * 0.9))
            return std::make_pair(false, is_heavy);
        heavy_tasks.push(task);
        break;
    case TaskType::Compact:
    case TaskType::Flush:
    case TaskType::PlaceIndex:
        is_heavy = false;
        // reserve some task space for heavy tasks
        if (max_task_num > 1 && light_tasks.size() >= static_cast<size_t>(max_task_num * 0.9))
            return std::make_pair(false, is_heavy);
        light_tasks.push(task);
        break;
    default:
        throw Exception(fmt::format("Unsupported task type: {}", magic_enum::enum_name(task.type)));
    }

    LOG_DEBUG(
        log_,
        "Segment task add to background task pool, segment={} task={} by_whom={}",
        task.segment->simpleInfo(),
        magic_enum::enum_name(task.type),
        magic_enum::enum_name(whom));
    return std::make_pair(true, is_heavy);
}

DeltaMergeStore::BackgroundTask DeltaMergeStore::MergeDeltaTaskPool::nextTask(bool is_heavy, const LoggerPtr & log_)
{
    std::scoped_lock lock(mutex);

    auto & tasks = is_heavy ? heavy_tasks : light_tasks;
    if (tasks.empty())
        return {};
    auto task = tasks.front();
    tasks.pop();

    LOG_DEBUG(log_, "Segment task pop from background task pool, segment={} task={}", task.segment->simpleInfo(), magic_enum::enum_name(task.type));

    return task;
}

// ================================================
//   DeltaMergeStore
// ================================================

namespace
{
// Actually we will always store a column of `_tidb_rowid`, no matter it
// exist in `table_columns` or not.
ColumnDefinesPtr generateStoreColumns(const ColumnDefines & table_columns, bool is_common_handle)
{
    auto columns = std::make_shared<ColumnDefines>();
    // First three columns are always _tidb_rowid, _INTERNAL_VERSION, _INTERNAL_DELMARK
    columns->emplace_back(getExtraHandleColumnDefine(is_common_handle));
    columns->emplace_back(getVersionColumnDefine());
    columns->emplace_back(getTagColumnDefine());
    // Add other columns
    for (const auto & col : table_columns)
    {
        if (col.name != EXTRA_HANDLE_COLUMN_NAME && col.name != VERSION_COLUMN_NAME && col.name != TAG_COLUMN_NAME)
            columns->emplace_back(col);
    }
    return columns;
}
} // namespace

DeltaMergeStore::Settings DeltaMergeStore::EMPTY_SETTINGS = DeltaMergeStore::Settings{.not_compress_columns = NotCompress{}};

DeltaMergeStore::DeltaMergeStore(Context & db_context,
                                 bool data_path_contains_database_name,
                                 const String & db_name_,
                                 const String & table_name_,
                                 TableID physical_table_id_,
                                 bool has_replica,
                                 const ColumnDefines & columns,
                                 const ColumnDefine & handle,
                                 bool is_common_handle_,
                                 size_t rowkey_column_size_,
                                 const Settings & settings_)
    : global_context(db_context.getGlobalContext())
    , path_pool(std::make_shared<StoragePathPool>(global_context.getPathPool().withTable(db_name_, table_name_, data_path_contains_database_name)))
    , settings(settings_)
    , db_name(db_name_)
    , table_name(table_name_)
    , physical_table_id(physical_table_id_)
    , is_common_handle(is_common_handle_)
    , rowkey_column_size(rowkey_column_size_)
    , original_table_handle_define(handle)
    , background_pool(db_context.getBackgroundPool())
    , blockable_background_pool(db_context.getBlockableBackgroundPool())
    , next_gc_check_key(is_common_handle ? RowKeyValue::COMMON_HANDLE_MIN_KEY : RowKeyValue::INT_HANDLE_MIN_KEY)
    , log(Logger::get(fmt::format("table_id={}", physical_table_id_)))
{
    replica_exist.store(has_replica);
    // for mock test, table_id_ should be DB::InvalidTableID
    NamespaceId ns_id = physical_table_id == DB::InvalidTableID ? TEST_NAMESPACE_ID : physical_table_id;

    LOG_INFO(log, "Restore DeltaMerge Store start");

    storage_pool = std::make_shared<StoragePool>(global_context,
                                                 ns_id,
                                                 *path_pool,
                                                 db_name_ + "." + table_name_);

    // Restore existing dm files and set capacity for path_pool.
    // Should be done before any background task setup.
    restoreStableFiles();

    original_table_columns.emplace_back(original_table_handle_define);
    original_table_columns.emplace_back(getVersionColumnDefine());
    original_table_columns.emplace_back(getTagColumnDefine());
    for (const auto & col : columns)
    {
        if (col.id != original_table_handle_define.id && col.id != VERSION_COLUMN_ID && col.id != TAG_COLUMN_ID)
            original_table_columns.emplace_back(col);
    }

    original_table_header = std::make_shared<Block>(toEmptyBlock(original_table_columns));
    store_columns = generateStoreColumns(original_table_columns, is_common_handle);

    auto dm_context = newDMContext(db_context, db_context.getSettingsRef());
    PageStorageRunMode page_storage_run_mode;
    try
    {
        page_storage_run_mode = storage_pool->restore(); // restore from disk
        if (const auto first_segment_entry = storage_pool->metaReader()->getPageEntry(DELTA_MERGE_FIRST_SEGMENT_ID);
            !first_segment_entry.isValid())
        {
            auto segment_id = storage_pool->newMetaPageId();
            if (segment_id != DELTA_MERGE_FIRST_SEGMENT_ID)
            {
                if (page_storage_run_mode == PageStorageRunMode::ONLY_V2)
                {
                    throw Exception(fmt::format("The first segment id should be {}", DELTA_MERGE_FIRST_SEGMENT_ID), ErrorCodes::LOGICAL_ERROR);
                }

                // In ONLY_V3 or MIX_MODE, If create a new DeltaMergeStore
                // Should used fixed DELTA_MERGE_FIRST_SEGMENT_ID to create first segment
                segment_id = DELTA_MERGE_FIRST_SEGMENT_ID;
            }

            auto first_segment = Segment::newSegment( //
                log,
                *dm_context,
                store_columns,
                RowKeyRange::newAll(is_common_handle, rowkey_column_size),
                segment_id,
                0);
            segments.emplace(first_segment->getRowKeyRange().getEnd(), first_segment);
            id_to_segment.emplace(segment_id, first_segment);
        }
        else
        {
            auto segment_id = DELTA_MERGE_FIRST_SEGMENT_ID;
            while (segment_id)
            {
                auto segment = Segment::restoreSegment(log, *dm_context, segment_id);
                segments.emplace(segment->getRowKeyRange().getEnd(), segment);
                id_to_segment.emplace(segment_id, segment);

                segment_id = segment->nextSegmentId();
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    setUpBackgroundTask(dm_context);

    LOG_INFO(log, "Restore DeltaMerge Store end, ps_run_mode={}", static_cast<UInt8>(page_storage_run_mode));
}

DeltaMergeStore::~DeltaMergeStore()
{
    LOG_INFO(log, "Release DeltaMerge Store start");

    shutdown();

    LOG_INFO(log, "Release DeltaMerge Store end");
}

void DeltaMergeStore::rename(String /*new_path*/, String new_database_name, String new_table_name)
{
    path_pool->rename(new_database_name, new_table_name);

    // TODO: replacing these two variables is not atomic, but could be good enough?
    table_name.swap(new_table_name);
    db_name.swap(new_database_name);
}

void DeltaMergeStore::dropAllSegments(bool keep_first_segment)
{
    auto dm_context = newDMContext(global_context, global_context.getSettingsRef());
    {
        std::unique_lock lock(read_write_mutex);
        auto segment_id = DELTA_MERGE_FIRST_SEGMENT_ID;
        std::stack<PageId> segment_ids;
        while (segment_id != 0)
        {
            segment_ids.push(segment_id);
            auto segment = id_to_segment[segment_id];
            segment_id = segment->nextSegmentId();
        }
        WriteBatches wbs(*storage_pool, dm_context->getWriteLimiter());
        while (!segment_ids.empty())
        {
            auto segment_id_to_drop = segment_ids.top();
            if (keep_first_segment && (segment_id_to_drop == DELTA_MERGE_FIRST_SEGMENT_ID))
            {
                // This must be the last segment to drop
                assert(segment_ids.size() == 1);
                break;
            }
            auto segment_to_drop = id_to_segment[segment_id_to_drop];
            segment_ids.pop();
            SegmentPtr previous_segment;
            SegmentPtr new_previous_segment;
            if (!segment_ids.empty())
            {
                // This is not the last segment, so we need to set previous segment's next_segment_id to 0 to indicate that this segment has been dropped
                auto previous_segment_id = segment_ids.top();
                previous_segment = id_to_segment[previous_segment_id];
                assert(previous_segment->nextSegmentId() == segment_id_to_drop);
                auto previous_lock = previous_segment->mustGetUpdateLock();

                FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_drop_segment);
                // No need to abandon previous_segment, because it's delta and stable is managed by the new_previous_segment.
                // Abandon previous_segment will actually abandon new_previous_segment
                //
                // And we need to use the previous_segment to manage the dropped segment's range,
                // because if tiflash crash in the middle of the drop table process, and when restoring this table at restart,
                // there are some possibilities that this table will trigger some background tasks,
                // and in these background tasks, it may check that all ranges of this table should be managed by some segment.
                new_previous_segment = previous_segment->dropNextSegment(wbs, segment_to_drop->getRowKeyRange());
                FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_after_drop_segment);
            }
            // The order to drop the meta and data of this segment doesn't matter,
            // Because there is no segment pointing to this segment,
            // so it won't be restored again even the drop process was interrupted by restart
            segments.erase(segment_to_drop->getRowKeyRange().getEnd());
            id_to_segment.erase(segment_id_to_drop);
            if (previous_segment)
            {
                assert(new_previous_segment);
                assert(previous_segment->segmentId() == new_previous_segment->segmentId());
                segments.erase(previous_segment->getRowKeyRange().getEnd());
                segments.emplace(new_previous_segment->getRowKeyRange().getEnd(), new_previous_segment);
                id_to_segment.erase(previous_segment->segmentId());
                id_to_segment.emplace(new_previous_segment->segmentId(), new_previous_segment);
            }
            auto drop_lock = segment_to_drop->mustGetUpdateLock();
            segment_to_drop->abandon(*dm_context);
            segment_to_drop->drop(global_context.getFileProvider(), wbs);
        }
    }
}

void DeltaMergeStore::clearData()
{
    // Remove all background task first
    shutdown();
    LOG_INFO(log, "Clear DeltaMerge segments data");
    // We don't drop the first segment in clearData, because if we drop it and tiflash crashes before drop the table's metadata,
    // when restart the table will try to restore the first segment but failed to do it which cause tiflash crash again.
    // The reason this happens is that even we delete all data in a PageStorage instance,
    // the call to PageStorage::getMaxId is still not 0 so tiflash treat it as an old table and will try to restore it's first segment.
    dropAllSegments(true);
    LOG_INFO(log, "Clear DeltaMerge segments data done");
}

void DeltaMergeStore::drop()
{
    // Remove all background task first
    shutdown();

    LOG_INFO(log, "Drop DeltaMerge removing data from filesystem");
    dropAllSegments(false);
    storage_pool->drop();

    // Drop data in storage path pool
    path_pool->drop(/*recursive=*/true, /*must_success=*/false);
    LOG_INFO(log, "Drop DeltaMerge done");
}

void DeltaMergeStore::shutdown()
{
    bool v = false;
    if (!shutdown_called.compare_exchange_strong(v, true))
        return;

    LOG_TRACE(log, "Shutdown DeltaMerge start");
    // Must shutdown storage path pool to make sure the DMFile remove callbacks
    // won't remove dmfiles unexpectly.
    path_pool->shutdown();
    // shutdown storage pool and clean up the local DMFile remove callbacks
    storage_pool->shutdown();

    background_pool.removeTask(background_task_handle);
    blockable_background_pool.removeTask(blockable_background_pool_handle);
    background_task_handle = nullptr;
    blockable_background_pool_handle = nullptr;
    LOG_TRACE(log, "Shutdown DeltaMerge end");
}

DMContextPtr DeltaMergeStore::newDMContext(const Context & db_context, const DB::Settings & db_settings, const String & tracing_id, const ScanContextPtr & scan_context_)
{
    std::shared_lock lock(read_write_mutex);

    // Here we use global context from db_context, instead of db_context directly.
    // Because db_context could be a temporary object and won't last long enough during the query process.
    // Like the context created by InterpreterSelectWithUnionQuery.
    auto * ctx = new DMContext(db_context.getGlobalContext(),
                               *path_pool,
                               *storage_pool,
                               latest_gc_safe_point.load(std::memory_order_acquire),
                               settings.not_compress_columns,
                               is_common_handle,
                               rowkey_column_size,
                               db_settings,
                               scan_context_,
                               tracing_id);
    return DMContextPtr(ctx);
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

            auto sub_col = c.cloneEmpty();
            sub_col.column = std::move(column);
            sub_col.column_id = c.column_id;
            sub_block.insert(std::move(sub_col));
        }
        return sub_block;
    }
}

// Add an extra handle column if the `handle_define` is used as the primary key
// TODO: consider merging it into `RegionBlockReader`?
Block DeltaMergeStore::addExtraColumnIfNeed(const Context & db_context, const ColumnDefine & handle_define, Block && block)
{
    if (pkIsHandle(handle_define))
    {
        if (!EXTRA_HANDLE_COLUMN_INT_TYPE->equals(*handle_define.type))
        {
            auto handle_pos = getPosByColumnId(block, handle_define.id);
            addColumnToBlock(block, //
                             EXTRA_HANDLE_COLUMN_ID,
                             EXTRA_HANDLE_COLUMN_NAME,
                             EXTRA_HANDLE_COLUMN_INT_TYPE,
                             EXTRA_HANDLE_COLUMN_INT_TYPE->createColumn());
            // Fill the new handle column with data in column[handle_pos] by applying cast.
            DefaultExecutable(FunctionToInt64::create(db_context)).execute(block, {handle_pos}, block.columns() - 1);
        }
        else
        {
            // If types are identical, `FunctionToInt64` just take reference to the original column.
            // We need a deep copy for the pk column or it will make trobule for later processing.
            auto pk_col_with_name = getByColumnId(block, handle_define.id);
            auto pk_column = pk_col_with_name.column;
            ColumnPtr handle_column = pk_column->cloneResized(pk_column->size());
            addColumnToBlock(block, //
                             EXTRA_HANDLE_COLUMN_ID,
                             EXTRA_HANDLE_COLUMN_NAME,
                             EXTRA_HANDLE_COLUMN_INT_TYPE,
                             handle_column);
        }
    }
    return std::move(block);
}

void DeltaMergeStore::write(const Context & db_context, const DB::Settings & db_settings, Block & block)
{
    LOG_TRACE(log, "Table write block, rows={} bytes={}", block.rows(), block.bytes());

    EventRecorder write_block_recorder(ProfileEvents::DMWriteBlock, ProfileEvents::DMWriteBlockNS);

    const auto rows = block.rows();
    if (rows == 0)
        return;

    auto dm_context = newDMContext(db_context, db_settings, "write");

<<<<<<< HEAD
=======
    if (db_context.getSettingsRef().dt_log_record_version)
    {
        const auto & ver_col = block.getByName(VERSION_COLUMN_NAME).column;
        const auto * ver = toColumnVectorDataPtr<UInt64>(ver_col);
        std::unordered_set<UInt64> dedup_ver;
        for (auto v : *ver)
        {
            dedup_ver.insert(v);
        }

        std::unordered_set<Int64> dedup_handles;
        auto extra_handle_col = tryGetByColumnId(block, EXTRA_HANDLE_COLUMN_ID);
        if (extra_handle_col.column)
        {
            if (extra_handle_col.type->getTypeId() == TypeIndex::Int64)
            {
                const auto * extra_handles = toColumnVectorDataPtr<Int64>(extra_handle_col.column);
                for (auto h : *extra_handles)
                {
                    dedup_handles.insert(h);
                }
            }
        }

        LOG_DEBUG(
            log,
            "region_id={} applied_index={} record_count={} versions={} handles={}",
            applied_status.region_id,
            applied_status.applied_index,
            block.rows(),
            dedup_ver,
            dedup_handles);
    }
>>>>>>> 89d1d73883 (KVStore: Log when meets any commit_ts < observed max_read_tso (#8991))
    const auto bytes = block.bytes();

    {
        // Sort the block by handle & version in ascending order.
        SortDescription sort;
        sort.emplace_back(EXTRA_HANDLE_COLUMN_NAME, 1, 0);
        sort.emplace_back(VERSION_COLUMN_NAME, 1, 0);

        if (rows > 1 && !isAlreadySorted(block, sort))
            stableSortBlock(block, sort);
    }

    Segments updated_segments;

    size_t offset = 0;
    size_t limit;
    const auto handle_column = block.getByName(EXTRA_HANDLE_COLUMN_NAME).column;
    auto rowkey_column = RowKeyColumnContainer(handle_column, is_common_handle);

    // Write block by segments
    while (offset != rows)
    {
        RowKeyValueRef start_key = rowkey_column.getRowKeyValue(offset);
        WriteBatches wbs(*storage_pool, db_context.getWriteLimiter());
        ColumnFilePtr write_column_file;
        RowKeyRange write_range;

        // Keep trying until succeeded.
        while (true)
        {
            // Find the segment according to current start_key
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(start_key);
                if (segment_it == segments.end())
                {
                    // todo print meaningful start row key
                    throw Exception(fmt::format("Failed to locate segment begin with start: {}", start_key.toDebugString()), ErrorCodes::LOGICAL_ERROR);
                }
                segment = segment_it->second;
            }

            FAIL_POINT_PAUSE(FailPoints::pause_when_writing_to_dt_store);

            // Do force merge or stop write if necessary.
            waitForWrite(dm_context, segment);
            if (segment->hasAbandoned())
                continue;

            const auto & rowkey_range = segment->getRowKeyRange();

            // The [offset, rows - offset] can be exceeding the Segment's rowkey_range. Cut the range
            // to fit the segment.
            auto [cur_offset, cur_limit] = rowkey_range.getPosRange(handle_column, offset, rows - offset);
            RUNTIME_CHECK_MSG(cur_offset == offset && cur_limit != 0,
                              "invalid cur_offset or cur_limit. is_common_handle={} start_key={} cur_offset={} cur_limit={} rows={} offset={} rowkey_range={}",
                              is_common_handle,
                              start_key.toRowKeyValue().toString(),
                              cur_offset,
                              cur_limit,
                              rows,
                              offset,
                              rowkey_range.toDebugString());

            limit = cur_limit;
            auto alloc_bytes = block.bytes(offset, limit);

            bool is_small = limit < dm_context->delta_cache_limit_rows / 4 && alloc_bytes < dm_context->delta_cache_limit_bytes / 4;
            // For small column files, data is appended to MemTableSet, then flushed later.
            // For large column files, data is directly written to PageStorage, while the ColumnFile entry is appended to MemTableSet.
            if (is_small)
            {
                if (segment->writeToCache(*dm_context, block, offset, limit))
                {
                    GET_METRIC(tiflash_storage_subtask_throughput_bytes, type_write_to_cache).Increment(alloc_bytes);
                    GET_METRIC(tiflash_storage_subtask_throughput_rows, type_write_to_cache).Increment(limit);
                    updated_segments.push_back(segment);
                    break;
                }
            }
            else
            {
                // If column file haven't been written, or the pk range has changed since last write, then write it and
                // delete former written column file.
                if (!write_column_file || (write_column_file && write_range != rowkey_range))
                {
                    wbs.rollbackWrittenLogAndData();
                    wbs.clear();

                    // In this case we will construct a ColumnFile that does not contain block data in the memory.
                    // The block data has been written to PageStorage in wbs.
                    write_column_file = ColumnFileTiny::writeColumnFile(*dm_context, block, offset, limit, wbs);
                    wbs.writeLogAndData();
                    write_range = rowkey_range;
                }

                // Write could fail, because other threads could already updated the instance. Like split/merge, merge delta.
                if (segment->writeToDisk(*dm_context, write_column_file))
                {
                    GET_METRIC(tiflash_storage_subtask_throughput_bytes, type_write_to_disk).Increment(alloc_bytes);
                    GET_METRIC(tiflash_storage_subtask_throughput_rows, type_write_to_disk).Increment(limit);
                    updated_segments.push_back(segment);
                    break;
                }
            }
        }

        offset += limit;
    }

    GET_METRIC(tiflash_storage_throughput_bytes, type_write).Increment(bytes);
    GET_METRIC(tiflash_storage_throughput_rows, type_write).Increment(rows);

    if (db_settings.dt_flush_after_write)
    {
        RowKeyRange merge_range = RowKeyRange::newNone(is_common_handle, rowkey_column_size);
        for (auto & segment : updated_segments)
            merge_range = merge_range.merge(segment->getRowKeyRange());
        flushCache(dm_context, merge_range);
    }

    fiu_do_on(FailPoints::random_exception_after_dt_write_done, {
        static int num_call = 0;
        if (num_call++ % 10 == 7)
            throw Exception("Fail point random_exception_after_dt_write_done is triggered.", ErrorCodes::FAIL_POINT_ERROR);
    });

    // TODO: Update the tracing_id before checkSegmentUpdate
    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);
}

void DeltaMergeStore::deleteRange(const Context & db_context, const DB::Settings & db_settings, const RowKeyRange & delete_range)
{
    LOG_INFO(log, "Table delete range, range={}", delete_range.toDebugString());

    EventRecorder write_block_recorder(ProfileEvents::DMDeleteRange, ProfileEvents::DMDeleteRangeNS);

    if (delete_range.none())
        return;

    auto dm_context = newDMContext(db_context, db_settings, "delete_range");

    Segments updated_segments;

    RowKeyRange cur_range = delete_range;

    while (!cur_range.none())
    {
        RowKeyRange segment_range;

        // Keep trying until succeeded.
        while (true)
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(cur_range.getStart());
                if (segment_it == segments.end())
                {
                    throw Exception(
                        fmt::format("Failed to locate segment begin with start in range: {}", cur_range.toDebugString()),
                        ErrorCodes::LOGICAL_ERROR);
                }
                segment = segment_it->second;
            }

            waitForDeleteRange(dm_context, segment);
            if (segment->hasAbandoned())
                continue;

            segment_range = segment->getRowKeyRange();

            // Write could fail, because other threads could already updated the instance. Like split/merge, merge delta.
            if (segment->write(*dm_context, delete_range.shrink(segment_range)))
            {
                updated_segments.push_back(segment);
                break;
            }
        }

        cur_range.setStart(segment_range.end);
        cur_range.setEnd(delete_range.end);
    }

    // TODO: Update the tracing_id before checkSegmentUpdate?
    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);
}

bool DeltaMergeStore::flushCache(const DMContextPtr & dm_context, const RowKeyRange & range, bool try_until_succeed)
{
    size_t sleep_ms = 5;

    RowKeyRange cur_range = range;
    while (!cur_range.none())
    {
        RowKeyRange segment_range;

        // Keep trying until succeeded if needed.
        while (true)
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(cur_range.getStart());
                if (segment_it == segments.end())
                {
                    throw Exception(
                        fmt::format("Failed to locate segment begin with start in range: {}", cur_range.toDebugString()),
                        ErrorCodes::LOGICAL_ERROR);
                }
                segment = segment_it->second;
            }
            segment_range = segment->getRowKeyRange();

            if (segment->flushCache(*dm_context))
            {
                break;
            }
            else if (!try_until_succeed)
            {
                return false;
            }

            // Flush could fail. Typical cases:
            // #1. The segment is abandoned (due to an update is finished)
            // #2. There is another flush in progress, for example, triggered in background
            // Let's sleep 5ms ~ 100ms and then retry flush again.
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            sleep_ms = std::min(sleep_ms * 2, 100);
        }

        cur_range.setStart(segment_range.end);
    }
    return true;
}

bool DeltaMergeStore::mergeDeltaAll(const Context & context)
{
    LOG_INFO(log, "Begin table mergeDeltaAll");

    auto dm_context = newDMContext(context, context.getSettingsRef(), /*tracing_id*/ "mergeDeltaAll");

    std::vector<SegmentPtr> all_segments;
    {
        std::shared_lock lock(read_write_mutex);
        for (auto & [range_end, segment] : segments)
        {
            (void)range_end;
            all_segments.push_back(segment);
        }
    }

    bool all_succ = true;
    for (auto & segment : all_segments)
    {
        bool succ = segmentMergeDelta(*dm_context, segment, MergeDeltaReason::Manual) != nullptr;
        all_succ = all_succ && succ;
    }

    LOG_INFO(log, "Finish table mergeDeltaAll: {}", all_succ);
    return all_succ;
}

std::optional<DM::RowKeyRange> DeltaMergeStore::mergeDeltaBySegment(const Context & context, const RowKeyValue & start_key)
{
    LOG_INFO(log, "Table mergeDeltaBySegment, start={}", start_key.toDebugString());

    SYNC_FOR("before_DeltaMergeStore::mergeDeltaBySegment");

    updateGCSafePoint();
    auto dm_context = newDMContext(context, context.getSettingsRef(),
                                   /*tracing_id*/ fmt::format("mergeDeltaBySegment_{}", latest_gc_safe_point.load(std::memory_order_relaxed)));

    size_t sleep_ms = 50;

    while (true)
    {
        SegmentPtr segment;
        {
            std::shared_lock lock(read_write_mutex);
            const auto segment_it = segments.upper_bound(start_key.toRowKeyValueRef());
            if (segment_it == segments.end())
            {
                return std::nullopt;
            }
            segment = segment_it->second;
        }

        if (segment->flushCache(*dm_context))
        {
            const auto new_segment = segmentMergeDelta(*dm_context, segment, MergeDeltaReason::Manual);
            if (new_segment)
            {
                const auto segment_end = new_segment->getRowKeyRange().end;
                if (unlikely(*segment_end.value <= *start_key.value))
                {
                    // The next start key must be > current start key
                    LOG_ERROR(log, "Assert new_segment.end {} > start {} failed", segment_end.toDebugString(), start_key.toDebugString());
                    throw Exception("Assert segment range failed", ErrorCodes::LOGICAL_ERROR);
                }
                return new_segment->getRowKeyRange();
            } // else: sleep and retry
        } // else: sleep and retry

        SYNC_FOR("before_DeltaMergeStore::mergeDeltaBySegment|retry_segment");

        // Typical cases:
        // #1. flushCache failed
        //    - The segment is abandoned (due to segment updated)
        //    - There is another flush in progress (e.g. triggered in background)
        // #2. segmentMergeDelta failed
        //    - The segment is abandoned (due to segment updated)
        //    - The segment is updating (e.g. a split-preparation is working, which occupies a for-write snapshot).
        // It could be possible to take seconds to finish the segment updating, so let's sleep for a short time
        // (50ms ~ 1000ms) and then retry.
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        sleep_ms = std::min(sleep_ms * 2, 1000);
    }
}

void DeltaMergeStore::compact(const Context & db_context, const RowKeyRange & range)
{
    auto dm_context = newDMContext(db_context, db_context.getSettingsRef(), /*tracing_id*/ "compact");

    RowKeyRange cur_range = range;
    while (!cur_range.none())
    {
        RowKeyRange segment_range;
        // Keep trying until succeeded.
        while (true)
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(cur_range.getStart());
                if (segment_it == segments.end())
                {
                    throw Exception(
                        fmt::format("Failed to locate segment begin with start in range: {}", cur_range.toDebugString()),
                        ErrorCodes::LOGICAL_ERROR);
                }
                segment = segment_it->second;
            }
            segment_range = segment->getRowKeyRange();

            // compact could fail.
            if (segment->compactDelta(*dm_context))
            {
                break;
            }
        }

        cur_range.setStart(segment_range.end);
    }
}

// Read data without mvcc filtering.
// just for debug
// readRaw is called under 'selraw  xxxx'
BlockInputStreams DeltaMergeStore::readRaw(const Context & db_context,
                                           const DB::Settings & db_settings,
                                           const ColumnDefines & columns_to_read,
                                           size_t num_streams,
                                           bool keep_order,
                                           const SegmentIdSet & read_segments,
                                           size_t extra_table_id_index)
{
    SegmentReadTasks tasks;

    auto dm_context = newDMContext(db_context, db_settings, fmt::format("read_raw_{}", db_context.getCurrentQueryId()));
    // If keep order is required, disable read thread.
    auto enable_read_thread = db_context.getSettingsRef().dt_enable_read_thread && !keep_order;
    {
        std::shared_lock lock(read_write_mutex);

        for (const auto & [handle, segment] : segments)
        {
            (void)handle;
            if (read_segments.empty() || read_segments.count(segment->segmentId()))
            {
                auto segment_snap = segment->createSnapshot(*dm_context, false, CurrentMetrics::DT_SnapshotOfReadRaw);
                if (unlikely(!segment_snap))
                    throw Exception("Failed to get segment snap", ErrorCodes::LOGICAL_ERROR);

                tasks.push_back(std::make_shared<SegmentReadTask>(segment, segment_snap, RowKeyRanges{segment->getRowKeyRange()}));
            }
        }
    }

    fiu_do_on(FailPoints::force_slow_page_storage_snapshot_release, {
        std::thread thread_hold_snapshots([this, tasks]() {
            LOG_WARNING(log, "failpoint force_slow_page_storage_snapshot_release begin");
            std::this_thread::sleep_for(std::chrono::seconds(5 * 60));
            (void)tasks;
            LOG_WARNING(log, "failpoint force_slow_page_storage_snapshot_release end");
        });
        thread_hold_snapshots.detach();
    });

    auto after_segment_read = [&](const DMContextPtr & dm_context_, const SegmentPtr & segment_) {
        this->checkSegmentUpdate(dm_context_, segment_, ThreadType::Read);
    };
    size_t final_num_stream = std::min(num_streams, tasks.size());
    String req_info;
    if (db_context.getDAGContext() != nullptr && db_context.getDAGContext()->isMPPTask())
        req_info = db_context.getDAGContext()->getMPPTaskId().toString();
    auto read_task_pool = std::make_shared<SegmentReadTaskPool>(
        physical_table_id,
        dm_context,
        columns_to_read,
        EMPTY_FILTER,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE,
        /* read_mode */ ReadMode::Raw,
        std::move(tasks),
        after_segment_read,
        req_info,
        enable_read_thread,
        final_num_stream);

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream;
        if (enable_read_thread)
        {
            stream = std::make_shared<UnorderedInputStream>(
                read_task_pool,
                columns_to_read,
                extra_table_id_index,
                physical_table_id,
                req_info);
        }
        else
        {
            stream = std::make_shared<DMSegmentThreadInputStream>(
                dm_context,
                read_task_pool,
                after_segment_read,
                columns_to_read,
                EMPTY_FILTER,
                std::numeric_limits<UInt64>::max(),
                DEFAULT_BLOCK_SIZE,
                /* read_mode */ ReadMode::Raw,
                extra_table_id_index,
                physical_table_id,
                req_info);
        }
        res.push_back(stream);
    }
    return res;
}

BlockInputStreams DeltaMergeStore::read(const Context & db_context,
                                        const DB::Settings & db_settings,
                                        const ColumnDefines & columns_to_read,
                                        const RowKeyRanges & sorted_ranges,
                                        size_t num_streams,
                                        UInt64 max_version,
                                        const RSOperatorPtr & filter,
                                        const String & tracing_id,
                                        bool keep_order,
                                        bool is_fast_scan,
                                        size_t expected_block_size,
                                        const SegmentIdSet & read_segments,
                                        size_t extra_table_id_index,
                                        const ScanContextPtr & scan_context)
{
    // Use the id from MPP/Coprocessor level as tracing_id
    auto dm_context = newDMContext(db_context, db_settings, tracing_id, scan_context);

    // If keep order is required, disable read thread.
    auto enable_read_thread = db_context.getSettingsRef().dt_enable_read_thread && !keep_order;
    // SegmentReadTaskScheduler and SegmentReadTaskPool use table_id + segment id as unique ID when read thread is enabled.
    // 'try_split_task' can result in several read tasks with the same id that can cause some trouble.
    // Also, too many read tasks of a segment with different small ranges is not good for data sharing cache.
    SegmentReadTasks tasks = getReadTasksByRanges(*dm_context, sorted_ranges, num_streams, read_segments, /*try_split_task =*/!enable_read_thread, scan_context);
    auto log_tracing_id = getLogTracingId(*dm_context);
    auto tracing_logger = log->getChild(log_tracing_id);
    LOG_INFO(tracing_logger,
             "Read create segment snapshot done, keep_order={} dt_enable_read_thread={} enable_read_thread={}",
             keep_order,
             db_context.getSettingsRef().dt_enable_read_thread,
             enable_read_thread);

    auto after_segment_read = [&](const DMContextPtr & dm_context_, const SegmentPtr & segment_) {
        // TODO: Update the tracing_id before checkSegmentUpdate?
        this->checkSegmentUpdate(dm_context_, segment_, ThreadType::Read);
    };

    GET_METRIC(tiflash_storage_read_tasks_count).Increment(tasks.size());
    size_t final_num_stream = std::max(1, std::min(num_streams, tasks.size()));
    auto read_task_pool = std::make_shared<SegmentReadTaskPool>(
        physical_table_id,
        dm_context,
        columns_to_read,
        filter,
        max_version,
        expected_block_size,
        /* read_mode = */ is_fast_scan ? ReadMode::Fast : ReadMode::Normal,
        std::move(tasks),
        after_segment_read,
        log_tracing_id,
        enable_read_thread,
        final_num_stream);

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream;
        if (enable_read_thread)
        {
            stream = std::make_shared<UnorderedInputStream>(
                read_task_pool,
                columns_to_read,
                extra_table_id_index,
                physical_table_id,
                log_tracing_id);
        }
        else
        {
            stream = std::make_shared<DMSegmentThreadInputStream>(
                dm_context,
                read_task_pool,
                after_segment_read,
                columns_to_read,
                filter,
                max_version,
                expected_block_size,
                /* read_mode = */ is_fast_scan ? ReadMode::Fast : ReadMode::Normal,
                extra_table_id_index,
                physical_table_id,
                log_tracing_id);
        }
        res.push_back(stream);
    }
    LOG_INFO(tracing_logger, "Read create stream done, pool_id={} num_streams={}", read_task_pool->poolId(), final_num_stream);

    return res;
}

size_t forceMergeDeltaRows(const DMContextPtr & dm_context)
{
    return dm_context->db_context.getSettingsRef().dt_segment_force_merge_delta_rows;
}

size_t forceMergeDeltaBytes(const DMContextPtr & dm_context)
{
    return dm_context->db_context.getSettingsRef().dt_segment_force_merge_delta_size;
}

size_t forceMergeDeltaDeletes(const DMContextPtr & dm_context)
{
    return dm_context->db_context.getSettingsRef().dt_segment_force_merge_delta_deletes;
}

void DeltaMergeStore::waitForWrite(const DMContextPtr & dm_context, const SegmentPtr & segment)
{
    size_t delta_rows = segment->getDelta()->getRows();
    size_t delta_bytes = segment->getDelta()->getBytes();

    // No need to stall the write stall if not exceeding the threshold of force merge.
    if (delta_rows < forceMergeDeltaRows(dm_context) && delta_bytes < forceMergeDeltaBytes(dm_context))
        return;

    // FIXME: checkSegmentUpdate will also count write stalls at each call.
    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_storage_write_stall_duration_seconds, type_write).Observe(watch.elapsedSeconds()); });

    size_t segment_bytes = segment->getEstimatedBytes();
    // The speed of delta merge in a very bad situation we assume. It should be a very conservative value.
    const size_t k10mb = 10 << 20;

    size_t stop_write_delta_rows = dm_context->db_context.getSettingsRef().dt_segment_stop_write_delta_rows;
    size_t stop_write_delta_bytes = dm_context->db_context.getSettingsRef().dt_segment_stop_write_delta_size;
    size_t wait_duration_factor = dm_context->db_context.getSettingsRef().dt_segment_wait_duration_factor;

    size_t sleep_ms;
    if (delta_rows >= stop_write_delta_rows || delta_bytes >= stop_write_delta_bytes)
    {
        // For stop write (hard limit), wait until segment is updated (e.g. delta is merged).
        sleep_ms = std::numeric_limits<size_t>::max();
    }
    else
    {
        // For force merge (soft limit), wait for a reasonable amount of time.
        // It is possible that the segment is still not updated after the wait.
        sleep_ms = static_cast<double>(segment_bytes) / k10mb * 1000 * wait_duration_factor;
    }

    // checkSegmentUpdate could do foreground merge delta, so call it before sleep.
    checkSegmentUpdate(dm_context, segment, ThreadType::Write);

    size_t sleep_step = 50;
    // Wait at most `sleep_ms` until the delta is merged.
    // Merge delta will replace the segment instance, causing `segment->hasAbandoned() == true`.
    while (!segment->hasAbandoned() && sleep_ms > 0)
    {
        size_t ms = std::min(sleep_ms, sleep_step);
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        sleep_ms -= ms;
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);
    }
}

void DeltaMergeStore::waitForDeleteRange(const DB::DM::DMContextPtr &, const DB::DM::SegmentPtr &)
{
    // TODO: maybe we should wait, if there are too many delete ranges?
}

void DeltaMergeStore::checkSegmentUpdate(const DMContextPtr & dm_context, const SegmentPtr & segment, ThreadType thread_type)
{
    fiu_do_on(FailPoints::skip_check_segment_update, { return; });

    if (segment->hasAbandoned())
        return;
    const auto & delta = segment->getDelta();

    size_t delta_saved_rows = delta->getRows(/* use_unsaved */ false);
    size_t delta_saved_bytes = delta->getBytes(/* use_unsaved */ false);
    size_t delta_check_rows = std::max(delta->updatesInDeltaTree(), delta_saved_rows);
    size_t delta_check_bytes = delta_saved_bytes;

    size_t delta_deletes = delta->getDeletes();

    size_t unsaved_rows = delta->getUnsavedRows();
    size_t unsaved_bytes = delta->getUnsavedBytes();

    size_t delta_rows = delta_saved_rows + unsaved_rows;
    size_t delta_bytes = delta_saved_bytes + unsaved_bytes;
    size_t segment_rows = segment->getEstimatedRows();
    size_t segment_bytes = segment->getEstimatedBytes();
    size_t column_file_count = delta->getColumnFileCount();

    size_t placed_delta_rows = delta->getPlacedDeltaRows();

    auto & delta_last_try_flush_rows = delta->getLastTryFlushRows();
    auto & delta_last_try_flush_bytes = delta->getLastTryFlushBytes();
    auto & delta_last_try_compact_column_files = delta->getLastTryCompactColumnFiles();
    auto & delta_last_try_merge_delta_rows = delta->getLastTryMergeDeltaRows();
    auto & delta_last_try_merge_delta_bytes = delta->getLastTryMergeDeltaBytes();
    auto & delta_last_try_split_rows = delta->getLastTrySplitRows();
    auto & delta_last_try_split_bytes = delta->getLastTrySplitBytes();
    auto & delta_last_try_place_delta_index_rows = delta->getLastTryPlaceDeltaIndexRows();

    auto segment_limit_rows = dm_context->segment_limit_rows;
    auto segment_limit_bytes = dm_context->segment_limit_bytes;
    auto delta_limit_rows = dm_context->delta_limit_rows;
    auto delta_limit_bytes = dm_context->delta_limit_bytes;
    auto delta_cache_limit_rows = dm_context->delta_cache_limit_rows;
    auto delta_cache_limit_bytes = dm_context->delta_cache_limit_bytes;

    bool should_background_flush = (unsaved_rows >= delta_cache_limit_rows || unsaved_bytes >= delta_cache_limit_bytes) //
        && (delta_rows - delta_last_try_flush_rows >= delta_cache_limit_rows
            || delta_bytes - delta_last_try_flush_bytes >= delta_cache_limit_bytes);
    bool should_foreground_flush = unsaved_rows >= delta_cache_limit_rows * 3 || unsaved_bytes >= delta_cache_limit_bytes * 3;
    /// For write thread, we want to avoid foreground flush to block the process of apply raft command.
    /// So we increase the threshold of foreground flush for write thread.
    if (thread_type == ThreadType::Write)
    {
        should_foreground_flush = unsaved_rows >= delta_cache_limit_rows * 10 || unsaved_bytes >= delta_cache_limit_bytes * 10;
    }

    bool should_background_merge_delta = ((delta_check_rows >= delta_limit_rows || delta_check_bytes >= delta_limit_bytes) //
                                          && (delta_rows - delta_last_try_merge_delta_rows >= delta_cache_limit_rows
                                              || delta_bytes - delta_last_try_merge_delta_bytes >= delta_cache_limit_bytes));
    bool should_foreground_merge_delta_by_rows_or_bytes
        = delta_check_rows >= forceMergeDeltaRows(dm_context) || delta_check_bytes >= forceMergeDeltaBytes(dm_context);
    bool should_foreground_merge_delta_by_deletes = delta_deletes >= forceMergeDeltaDeletes(dm_context);

    // Note that, we must use || to combine rows and bytes checks in split check, and use && in merge check.
    // Otherwise, segments could be split and merged over and over again.
    // Do background split in the following two cases:
    //   1. The segment is large enough, and there are some data in the delta layer. (A hot segment which is large enough)
    //   2. The segment is too large. (A segment which is too large, although it is cold)
    bool should_bg_split = ((segment_rows >= segment_limit_rows * 2 || segment_bytes >= segment_limit_bytes * 2)
                            && (delta_rows - delta_last_try_split_rows >= delta_cache_limit_rows
                                || delta_bytes - delta_last_try_split_bytes >= delta_cache_limit_bytes))
        || (segment_rows >= segment_limit_rows * 3 || segment_bytes >= segment_limit_bytes * 3);

    // Don't do compact on starting up.
    bool should_compact = (thread_type != ThreadType::Init) && std::max(static_cast<Int64>(column_file_count) - delta_last_try_compact_column_files, 0) >= 15;

    // Don't do background place index if we limit DeltaIndex cache.
    bool should_place_delta_index = !dm_context->db_context.isDeltaIndexLimited()
        && (delta_rows - placed_delta_rows >= delta_cache_limit_rows * 3
            && delta_rows - delta_last_try_place_delta_index_rows >= delta_cache_limit_rows);

    fiu_do_on(FailPoints::force_triggle_background_merge_delta, { should_background_merge_delta = true; });
    fiu_do_on(FailPoints::force_triggle_foreground_flush, { should_foreground_flush = true; });

    auto try_add_background_task = [&](const BackgroundTask & task) {
        if (shutdown_called.load(std::memory_order_relaxed))
            return;

        auto [added, heavy] = background_tasks.tryAddTask(task, thread_type, std::max(id_to_segment.size() * 2, background_pool.getNumberOfThreads() * 3), log);
        // Prevent too many tasks.
        if (!added)
            return;
        if (heavy)
            blockable_background_pool_handle->wake();
        else
            background_task_handle->wake();
    };

    /// Flush is always try first.
    if (thread_type != ThreadType::Read)
    {
        if (should_foreground_flush)
        {
            Stopwatch watch;
            SCOPE_EXIT({
                // We may be flushing in BG threads. Do not count them as write stalls.
                if (thread_type == ThreadType::Write)
                {
                    // FIXME: We'd better count write stall duration for per-write, instead of per-call,
                    //   in order to produce a meaningful value.
                    GET_METRIC(tiflash_storage_write_stall_duration_seconds, type_flush).Observe(watch.elapsedSeconds());
                }
            });

            delta_last_try_flush_rows = delta_rows;
            delta_last_try_flush_bytes = delta_bytes;
            LOG_DEBUG(log, "Foreground flush cache in checkSegmentUpdate, thread={} segment={}", thread_type, segment->info());
            segment->flushCache(*dm_context);
        }
        else if (should_background_flush)
        {
            /// It's meaningless to add more flush tasks if the segment is flushing.
            /// Because only one flush task can proceed at any time.
            /// And after the current flush task finished,
            /// it will call `checkSegmentUpdate` again to check whether there is more flush task to do.
            if (!segment->isFlushing())
            {
                delta_last_try_flush_rows = delta_rows;
                delta_last_try_flush_bytes = delta_bytes;
                try_add_background_task(BackgroundTask{TaskType::Flush, dm_context, segment});
            }
        }
    }

    // Need to check the latest delta (maybe updated after foreground flush). If it is updating by another thread,
    // give up adding more tasks on this version of delta.
    if (segment->getDelta()->isUpdating())
        return;

    auto try_fg_merge_delta = [&]() -> SegmentPtr {
        // If the table is already dropped, don't trigger foreground merge delta when executing `remove region peer`,
        // or the raft-log apply threads may be blocked.
        if ((should_foreground_merge_delta_by_rows_or_bytes || should_foreground_merge_delta_by_deletes) && replica_exist.load())
        {
            delta_last_try_merge_delta_rows = delta_rows;

            assert(thread_type == ThreadType::Write);

            Stopwatch watch;
            SCOPE_EXIT({
                // FIXME: We'd better count write stall duration for per-write, instead of per-call,
                //   in order to produce a meaningful value.
                if (should_foreground_merge_delta_by_rows_or_bytes)
                    GET_METRIC(tiflash_storage_write_stall_duration_seconds, type_delta_merge_by_write).Observe(watch.elapsedSeconds());
                if (should_foreground_merge_delta_by_deletes)
                    GET_METRIC(tiflash_storage_write_stall_duration_seconds, type_delta_merge_by_delete_range).Observe(watch.elapsedSeconds());
            });

            return segmentMergeDelta(*dm_context, segment, MergeDeltaReason::ForegroundWrite);
        }
        return {};
    };
    auto try_bg_merge_delta = [&]() {
        if (should_background_merge_delta)
        {
            delta_last_try_merge_delta_rows = delta_rows;
            try_add_background_task(BackgroundTask{TaskType::MergeDelta, dm_context, segment});
            return true;
        }
        return false;
    };
    auto try_bg_split = [&](const SegmentPtr & seg) {
        if (should_bg_split && !seg->isSplitForbidden())
        {
            delta_last_try_split_rows = delta_rows;
            delta_last_try_split_bytes = delta_bytes;
            try_add_background_task(BackgroundTask{TaskType::Split, dm_context, seg});
            return true;
        }
        return false;
    };
    auto try_fg_split = [&](const SegmentPtr & my_segment) {
        auto my_segment_size = my_segment->getEstimatedBytes();
        auto my_should_split = my_segment_size >= dm_context->segment_force_split_bytes;
        if (my_should_split && !my_segment->isSplitForbidden())
        {
            Stopwatch watch;
            SCOPE_EXIT({
                // FIXME: We'd better count write stall duration for per-write, instead of per-call,
                //   in order to produce a meaningful value.
                GET_METRIC(tiflash_storage_write_stall_duration_seconds, type_split).Observe(watch.elapsedSeconds());
            });

            return segmentSplit(*dm_context, my_segment, SegmentSplitReason::ForegroundWrite).first != nullptr;
        }
        return false;
    };
    auto try_bg_compact = [&]() {
        /// Compact task should be a really low priority task.
        /// And if the segment is flushing,
        /// we should avoid adding background compact task to reduce lock contention on the segment and save disk throughput.
        /// And after the current flush task complete,
        /// it will call `checkSegmentUpdate` again to check whether there is other kinds of task to do.
        if (should_compact && !segment->isFlushing())
        {
            delta_last_try_compact_column_files = column_file_count;
            try_add_background_task(BackgroundTask{TaskType::Compact, dm_context, segment});
            return true;
        }
        return false;
    };
    auto try_place_delta_index = [&]() {
        if (should_place_delta_index)
        {
            delta_last_try_place_delta_index_rows = delta_rows;
            try_add_background_task(BackgroundTask{TaskType::PlaceIndex, dm_context, segment});
            return true;
        }
        return false;
    };

    /// If current thread is write thread, check foreground merge delta.
    /// If current thread is background merge delta thread, then try split first.
    /// For other threads, try in order: background merge delta -> background split -> background merge -> background compact.

    if (thread_type == ThreadType::Write)
    {
        if (try_fg_split(segment))
            return;

        if (SegmentPtr new_segment = try_fg_merge_delta(); new_segment)
        {
            // After merge delta, we better check split immediately.
            if (try_bg_split(new_segment))
                return;
        }
    }
    else if (thread_type == ThreadType::BG_MergeDelta)
    {
        if (try_bg_split(segment))
            return;
    }

    if (dm_context->enable_logical_split)
    {
        // Logical split point is calculated based on stable. Always try to merge delta into the stable
        // before logical split is good for calculating the split point.
        if (try_bg_merge_delta())
            return;
        if (try_bg_split(segment))
            return;
    }
    else
    {
        // During the physical split delta will be merged, so we prefer physical split over merge delta.
        if (try_bg_split(segment))
            return;
        if (try_bg_merge_delta())
            return;
    }
    if (try_bg_compact())
        return;
    if (try_place_delta_index())
        return;

    // The segment does not need any updates for now.
}

void DeltaMergeStore::check(const Context & /*db_context*/)
{
    std::shared_lock lock(read_write_mutex);

    UInt64 next_segment_id = DELTA_MERGE_FIRST_SEGMENT_ID;
    RowKeyRange last_range = RowKeyRange::newAll(is_common_handle, rowkey_column_size);
    RowKeyValueRef last_end = last_range.getStart();
    for (const auto & [end, segment] : segments)
    {
        (void)end;

        auto segment_id = segment->segmentId();
        auto range = segment->getRowKeyRange();

        if (next_segment_id != segment_id)
        {
            FmtBuffer fmt_buf;
            fmt_buf.append("Check failed. Segments: ");
            fmt_buf.joinStr(
                segments.begin(),
                segments.end(),
                [](const auto & id_seg, FmtBuffer & fb) { fb.append(id_seg.second->info()); },
                ",");
            LOG_ERROR(log, fmt_buf.toString());

            throw Exception(fmt::format("Segment [{}] is expected to have id [{}]", segment_id, next_segment_id));
        }
        if (compare(last_end.data, last_end.size, range.getStart().data, range.getStart().size) != 0)
            throw Exception(
                fmt::format("Segment [{}:{}] is expected to have the same start edge value like the end edge value in {}",
                            segment_id,
                            range.toDebugString(),
                            last_range.toDebugString()));

        last_range = range;
        last_end = last_range.getEnd();
        next_segment_id = segment->nextSegmentId();
    }
    if (!last_range.isEndInfinite())
        throw Exception(fmt::format("Last range {} is expected to have infinite end edge", last_range.toDebugString()));
}

BlockPtr DeltaMergeStore::getHeader() const
{
    return std::atomic_load<Block>(&original_table_header);
}

void DeltaMergeStore::applyAlters(
    const AlterCommands & commands,
    const OptionTableInfoConstRef table_info,
    ColumnID & max_column_id_used,
    const Context & /* context */)
{
    std::unique_lock lock(read_write_mutex);

    FAIL_POINT_PAUSE(FailPoints::pause_when_altering_dt_store);

    ColumnDefines new_original_table_columns(original_table_columns.begin(), original_table_columns.end());
    for (const auto & command : commands)
    {
        applyAlter(new_original_table_columns, command, table_info, max_column_id_used);
    }

    if (table_info)
    {
        // Update primary keys from TiDB::TableInfo when pk_is_handle = true
        // todo update the column name in rowkey_columns
        std::vector<String> pk_names;
        for (const auto & col : table_info->get().columns)
        {
            if (col.hasPriKeyFlag())
            {
                pk_names.emplace_back(col.name);
            }
        }
        if (table_info->get().pk_is_handle && pk_names.size() == 1)
        {
            // Only update primary key name if pk is handle and there is only one column with
            // primary key flag
            original_table_handle_define.name = pk_names[0];
        }
        if (table_info.value().get().replica_info.count == 0)
        {
            replica_exist.store(false);
        }
    }

    auto new_store_columns = generateStoreColumns(new_original_table_columns, is_common_handle);

    original_table_columns.swap(new_original_table_columns);
    store_columns.swap(new_store_columns);

    std::atomic_store(&original_table_header, std::make_shared<Block>(toEmptyBlock(original_table_columns)));
}


SortDescription DeltaMergeStore::getPrimarySortDescription() const
{
    std::shared_lock lock(read_write_mutex);

    SortDescription desc;
    desc.emplace_back(original_table_handle_define.name, /* direction_= */ 1, /* nulls_direction_= */ 1);
    return desc;
}

void DeltaMergeStore::restoreStableFiles()
{
    LOG_DEBUG(log, "Loading dt files");

    DMFile::ListOptions options;
    options.only_list_can_gc = false; // We need all files to restore the bytes on disk
    options.clean_up = true;
    auto file_provider = global_context.getFileProvider();
    auto path_delegate = path_pool->getStableDiskDelegator();
    for (const auto & root_path : path_delegate.listPaths())
    {
        for (const auto & file_id : DMFile::listAllInPath(file_provider, root_path, options))
        {
            auto dmfile = DMFile::restore(file_provider, file_id, /* page_id= */ 0, root_path, DMFile::ReadMetaMode::diskSizeOnly());
            path_delegate.addDTFile(file_id, dmfile->getBytesOnDisk(), root_path);
        }
    }
}

SegmentReadTasks DeltaMergeStore::getReadTasksByRanges(
    DMContext & dm_context,
    const RowKeyRanges & sorted_ranges,
    size_t expected_tasks_count,
    const SegmentIdSet & read_segments,
    bool try_split_task,
    const ScanContextPtr & scan_context)
{
    SegmentReadTasks tasks;
    Stopwatch watch;

    std::shared_lock lock(read_write_mutex);

    auto range_it = sorted_ranges.begin();
    auto seg_it = segments.upper_bound(range_it->getStart());

    RUNTIME_CHECK_MSG(seg_it != segments.end(), "Failed to locate segment begin with start in range: {}", range_it->toDebugString());

    while (range_it != sorted_ranges.end() && seg_it != segments.end())
    {
        const auto & req_range = *range_it;
        const auto & seg_range = seg_it->second->getRowKeyRange();
        if (req_range.intersect(seg_range) && (read_segments.empty() || read_segments.count(seg_it->second->segmentId())))
        {
            if (tasks.empty() || tasks.back()->segment != seg_it->second)
            {
                auto segment = seg_it->second;
                auto segment_snap = segment->createSnapshot(dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
                RUNTIME_CHECK_MSG(segment_snap, "Failed to get segment snap");
                tasks.push_back(std::make_shared<SegmentReadTask>(segment, segment_snap));
            }

            tasks.back()->addRange(req_range);

            if (req_range.getEnd() < seg_range.getEnd())
            {
                ++range_it;
            }
            else if (seg_range.getEnd() < req_range.getEnd())
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
            if (req_range.getEnd() < seg_range.getStart())
                ++range_it;
            else
                ++seg_it;
        }
    }

    // how many segments involved for the given key ranges
    const auto tasks_before_split = tasks.size();
    if (try_split_task)
    {
        /// Try to make task number larger or equal to expected_tasks_count.
        tasks = SegmentReadTask::trySplitReadTasks(tasks, expected_tasks_count);
    }

    size_t total_ranges = 0;
    for (auto & task : tasks)
    {
        /// Merge continuously ranges.
        task->mergeRanges();
        total_ranges += task->ranges.size();
    }

    if (scan_context)
    {
        scan_context->num_segments += tasks_before_split;
        scan_context->num_read_tasks += tasks.size();
    }

    auto tracing_logger = log->getChild(getLogTracingId(dm_context));
    LOG_INFO(
        tracing_logger,
        "Segment read tasks build done, cost={}ms sorted_ranges={} n_tasks_before_split={} n_tasks_final={} n_ranges_final={}",
        watch.elapsedMilliseconds(),
        sorted_ranges.size(),
        tasks_before_split,
        tasks.size(),
        total_ranges);

    return tasks;
}

String DeltaMergeStore::getLogTracingId(const DMContext & dm_ctx)
{
    if (likely(!dm_ctx.tracing_id.empty()))
    {
        return dm_ctx.tracing_id;
    }
    else
    {
        return fmt::format("table_id={}", physical_table_id);
    }
}
} // namespace DM
} // namespace DB

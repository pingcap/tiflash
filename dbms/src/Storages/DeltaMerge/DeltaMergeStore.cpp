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

#include <Common/FailPoint.h>
#include <Common/FmtUtils.h>
#include <Common/Logger.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/assert_cast.h>
#include <Core/SortDescription.h>
#include <Functions/FunctionsConversion.h>
#include <Interpreters/sortBlock.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMSegmentThreadInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
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

#if USE_TCMALLOC
#include <gperftools/malloc_extension.h>
#endif

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
extern const Metric DT_DeltaMerge;
extern const Metric DT_SegmentSplit;
extern const Metric DT_SegmentMerge;
extern const Metric DT_DeltaMergeTotalBytes;
extern const Metric DT_DeltaMergeTotalRows;
extern const Metric DT_SnapshotOfRead;
extern const Metric DT_SnapshotOfReadRaw;
extern const Metric DT_SnapshotOfSegmentSplit;
extern const Metric DT_SnapshotOfSegmentMerge;
extern const Metric DT_SnapshotOfDeltaMerge;
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
extern const char pause_before_dt_background_delta_merge[];
extern const char pause_until_dt_background_delta_merge[];
extern const char pause_when_writing_to_dt_store[];
extern const char pause_when_ingesting_to_dt_store[];
extern const char pause_when_altering_dt_store[];
extern const char force_triggle_background_merge_delta[];
extern const char force_triggle_foreground_flush[];
extern const char force_set_segment_ingest_packs_fail[];
extern const char segment_merge_after_ingest_packs[];
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
    case TaskType::Merge:
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
        throw Exception(fmt::format("Unsupported task type: {}", toString(task.type)));
    }

    LOG_FMT_DEBUG(
        log_,
        "Segment [{}] task [{}] add to background task pool by [{}]",
        task.segment->segmentId(),
        toString(task.type),
        toString(whom));
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

    LOG_FMT_DEBUG(log_, "Segment [{}] task [{}] pop from background task pool", task.segment->segmentId(), toString(task.type));

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
                                 const ColumnDefines & columns,
                                 const ColumnDefine & handle,
                                 bool is_common_handle_,
                                 size_t rowkey_column_size_,
                                 const Settings & settings_)
    : global_context(db_context.getGlobalContext())
    , path_pool(global_context.getPathPool().withTable(db_name_, table_name_, data_path_contains_database_name))
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
    , hash_salt(++DELTA_MERGE_STORE_HASH_SALT)
    , log(Logger::get("DeltaMergeStore", fmt::format("{}.{}", db_name, table_name)))
{
    // for mock test, table_id_ should be DB::InvalidTableID
    NamespaceId ns_id = physical_table_id == DB::InvalidTableID ? TEST_NAMESPACE_ID : physical_table_id;

    LOG_FMT_INFO(log, "Restore DeltaMerge Store start [{}.{}] [table_id = {}]", db_name, table_name, physical_table_id);

    storage_pool = std::make_shared<StoragePool>(global_context,
                                                 ns_id,
                                                 path_pool,
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

            auto first_segment
                = Segment::newSegment(*dm_context, store_columns, RowKeyRange::newAll(is_common_handle, rowkey_column_size), segment_id, 0);
            segments.emplace(first_segment->getRowKeyRange().getEnd(), first_segment);
            id_to_segment.emplace(segment_id, first_segment);
        }
        else
        {
            auto segment_id = DELTA_MERGE_FIRST_SEGMENT_ID;
            while (segment_id)
            {
                auto segment = Segment::restoreSegment(*dm_context, segment_id);
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

    LOG_FMT_INFO(log, "Restore DeltaMerge Store end [{}.{}], [ps_run_mode={}]", db_name, table_name, static_cast<UInt8>(page_storage_run_mode));
}

DeltaMergeStore::~DeltaMergeStore()
{
    LOG_FMT_INFO(log, "Release DeltaMerge Store start [{}.{}]", db_name, table_name);

    shutdown();

    LOG_FMT_INFO(log, "Release DeltaMerge Store end [{}.{}]", db_name, table_name);
}

void DeltaMergeStore::setUpBackgroundTask(const DMContextPtr & dm_context)
{
    ExternalPageCallbacks callbacks;
    // V2 callbacks for cleaning DTFiles
    callbacks.scanner = [this]() {
        ExternalPageCallbacks::PathAndIdsVec path_and_ids_vec;
        auto delegate = path_pool.getStableDiskDelegator();
        DMFile::ListOptions options;
        options.only_list_can_gc = true;
        for (auto & root_path : delegate.listPaths())
        {
            auto & path_and_ids = path_and_ids_vec.emplace_back();
            path_and_ids.first = root_path;
            auto file_ids_in_current_path = DMFile::listAllInPath(global_context.getFileProvider(), root_path, options);
            for (auto id : file_ids_in_current_path)
                path_and_ids.second.insert(id);
        }
        return path_and_ids_vec;
    };
    callbacks.remover = [this](const ExternalPageCallbacks::PathAndIdsVec & path_and_ids_vec, const std::set<PageId> & valid_ids) {
        auto delegate = path_pool.getStableDiskDelegator();
        for (const auto & [path, ids] : path_and_ids_vec)
        {
            for (auto id : ids)
            {
                if (valid_ids.count(id))
                    continue;

                // Note that page_id is useless here.
                auto dmfile = DMFile::restore(global_context.getFileProvider(), id, /* page_id= */ 0, path, DMFile::ReadMetaMode::none());
                if (dmfile->canGC())
                {
                    delegate.removeDTFile(dmfile->fileId());
                    dmfile->remove(global_context.getFileProvider());
                }

                LOG_FMT_INFO(log, "GC removed useless dmfile: {}", dmfile->path());
            }
        }
    };
    callbacks.ns_id = storage_pool->getNamespaceId();
    // remember to unregister it when shutdown
    storage_pool->dataRegisterExternalPagesCallbacks(callbacks);
    storage_pool->enableGC();

    background_task_handle = background_pool.addTask([this] { return handleBackgroundTask(false); });

    blockable_background_pool_handle = blockable_background_pool.addTask([this] { return handleBackgroundTask(true); });

    // Do place delta index.
    for (auto & [end, segment] : segments)
    {
        (void)end;
        checkSegmentUpdate(dm_context, segment, ThreadType::Init);
    }

    // Wake up to do place delta index tasks.
    background_task_handle->wake();
    blockable_background_pool_handle->wake();
}

void DeltaMergeStore::rename(String /*new_path*/, bool clean_rename, String new_database_name, String new_table_name)
{
    if (clean_rename)
    {
        path_pool.rename(new_database_name, new_table_name, clean_rename);
    }
    else
    {
        LOG_FMT_WARNING(log, "Applying heavy renaming for table {}.{} to {}.{}", db_name, table_name, new_database_name, new_table_name);

        // Remove all background task first
        shutdown();
        path_pool.rename(new_database_name, new_table_name, clean_rename); // rename for multi-disk
    }

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
    LOG_FMT_INFO(log, "Clear DeltaMerge segments data [{}.{}]", db_name, table_name);
    // We don't drop the first segment in clearData, because if we drop it and tiflash crashes before drop the table's metadata,
    // when restart the table will try to restore the first segment but failed to do it which cause tiflash crash again.
    // The reason this happens is that even we delete all data in a PageStorage instance,
    // the call to PageStorage::getMaxId is still not 0 so tiflash treat it as an old table and will try to restore it's first segment.
    dropAllSegments(true);
    LOG_FMT_INFO(log, "Clear DeltaMerge segments data done [{}.{}]", db_name, table_name);
}

void DeltaMergeStore::drop()
{
    // Remove all background task first
    shutdown();

    LOG_FMT_INFO(log, "Drop DeltaMerge removing data from filesystem [{}.{}]", db_name, table_name);
    dropAllSegments(false);
    storage_pool->drop();

    // Drop data in storage path pool
    path_pool.drop(/*recursive=*/true, /*must_success=*/false);
    LOG_FMT_INFO(log, "Drop DeltaMerge done [{}.{}]", db_name, table_name);

#if USE_TCMALLOC
    // Reclaim memory.
    MallocExtension::instance()->ReleaseFreeMemory();
#endif
}

void DeltaMergeStore::shutdown()
{
    bool v = false;
    if (!shutdown_called.compare_exchange_strong(v, true))
        return;

    LOG_FMT_TRACE(log, "Shutdown DeltaMerge start [{}.{}]", db_name, table_name);
    // shutdown before unregister to avoid conflict between this thread and background gc thread on the `ExternalPagesCallbacks`
    // because PageStorage V2 doesn't have any lock protection on the `ExternalPagesCallbacks`.(The order doesn't matter for V3)
    storage_pool->shutdown();
    storage_pool->dataUnregisterExternalPagesCallbacks(storage_pool->getNamespaceId());

    background_pool.removeTask(background_task_handle);
    blockable_background_pool.removeTask(blockable_background_pool_handle);
    background_task_handle = nullptr;
    blockable_background_pool_handle = nullptr;
    LOG_FMT_TRACE(log, "Shutdown DeltaMerge end [{}.{}]", db_name, table_name);
}

DMContextPtr DeltaMergeStore::newDMContext(const Context & db_context, const DB::Settings & db_settings, const String & tracing_id)
{
    std::shared_lock lock(read_write_mutex);

    // Here we use global context from db_context, instead of db_context directly.
    // Because db_context could be a temporary object and won't last long enough during the query process.
    // Like the context created by InterpreterSelectWithUnionQuery.
    auto * ctx = new DMContext(db_context.getGlobalContext(),
                               path_pool,
                               *storage_pool,
                               hash_salt,
                               latest_gc_safe_point.load(std::memory_order_acquire),
                               settings.not_compress_columns,
                               is_common_handle,
                               rowkey_column_size,
                               db_settings,
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
    LOG_FMT_TRACE(log, "table: {}.{}, rows: {}", db_name, table_name, block.rows());

    EventRecorder write_block_recorder(ProfileEvents::DMWriteBlock, ProfileEvents::DMWriteBlockNS);

    const auto rows = block.rows();
    if (rows == 0)
        return;

    auto dm_context = newDMContext(db_context, db_settings, "write");

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
            if (unlikely(cur_offset != offset))
                throw Exception("cur_offset does not equal to offset", ErrorCodes::LOGICAL_ERROR);

            limit = cur_limit;
            auto alloc_bytes = block.bytes(offset, limit);

            bool is_small = limit < dm_context->delta_cache_limit_rows / 4 && alloc_bytes < dm_context->delta_cache_limit_bytes / 4;
            // For small column files, data is appended to MemTableSet, then flushed later.
            // For large column files, data is directly written to PageStorage, while the ColumnFile entry is appended to MemTableSet.
            if (is_small)
            {
                if (segment->writeToCache(*dm_context, block, offset, limit))
                {
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

std::tuple<String, PageId> DeltaMergeStore::preAllocateIngestFile()
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return {};

    auto delegator = path_pool.getStableDiskDelegator();
    auto parent_path = delegator.choosePath();
    auto new_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    return {parent_path, new_id};
}

void DeltaMergeStore::preIngestFile(const String & parent_path, const PageId file_id, size_t file_size)
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return;

    auto delegator = path_pool.getStableDiskDelegator();
    delegator.addDTFile(file_id, file_size, parent_path);
}

void DeltaMergeStore::ingestFiles(
    const DMContextPtr & dm_context,
    const RowKeyRange & range,
    const PageIds & file_ids,
    bool clear_data_in_range)
{
    if (unlikely(shutdown_called.load(std::memory_order_relaxed)))
    {
        const auto msg = fmt::format("try to ingest files into a shutdown table: {}.{}", db_name, table_name);
        LOG_FMT_WARNING(log, "{}", msg);
        throw Exception(msg);
    }

    EventRecorder write_block_recorder(ProfileEvents::DMWriteFile, ProfileEvents::DMWriteFileNS);

    auto delegate = dm_context->path_pool.getStableDiskDelegator();
    auto file_provider = dm_context->db_context.getFileProvider();

    size_t rows = 0;
    size_t bytes = 0;
    size_t bytes_on_disk = 0;

    DMFiles files;
    for (auto file_id : file_ids)
    {
        auto file_parent_path = delegate.getDTFilePath(file_id);

        // we always create a ref file to this DMFile with all meta info restored later, so here we just restore meta info to calculate its' memory and disk size
        auto file = DMFile::restore(file_provider, file_id, file_id, file_parent_path, DMFile::ReadMetaMode::memoryAndDiskSize());
        rows += file->getRows();
        bytes += file->getBytes();
        bytes_on_disk += file->getBytesOnDisk();

        files.emplace_back(std::move(file));
    }

    LOG_FMT_INFO(
        log,
        "table: {}.{}, rows: {}, bytes: {}, bytes on disk: {}, region range: {}, clear_data: {}",
        db_name,
        table_name,
        rows,
        bytes,
        bytes_on_disk,
        range.toDebugString(),
        clear_data_in_range);

    Segments updated_segments;
    RowKeyRange cur_range = range;

    // Put the ingest file ids into `storage_pool` and use ref id in each segments to ensure the atomic
    // of ingesting.
    // Check https://github.com/pingcap/tics/issues/2040 for more details.
    // TODO: If tiflash crash during the middle of ingesting, we may leave some DTFiles on disk and
    // they can not be deleted. We should find a way to cleanup those files.
    WriteBatches ingest_wbs(*storage_pool, dm_context->getWriteLimiter());
    if (!files.empty())
    {
        for (const auto & file : files)
        {
            ingest_wbs.data.putExternal(file->fileId(), 0);
        }
        ingest_wbs.writeLogAndData();
        ingest_wbs.setRollback(); // rollback if exception thrown
    }

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

            FAIL_POINT_PAUSE(FailPoints::pause_when_ingesting_to_dt_store);
            waitForWrite(dm_context, segment);
            if (segment->hasAbandoned())
                continue;

            segment_range = segment->getRowKeyRange();

            // Write could fail, because other threads could already updated the instance. Like split/merge, merge delta.
            ColumnFiles column_files;
            WriteBatches wbs(*storage_pool, dm_context->getWriteLimiter());

            for (const auto & file : files)
            {
                /// Generate DMFile instance with a new ref_id pointed to the file_id.
                auto file_id = file->fileId();
                const auto & file_parent_path = file->parentPath();
                auto page_id = storage_pool->newDataPageIdForDTFile(delegate, __PRETTY_FUNCTION__);

                auto ref_file = DMFile::restore(file_provider, file_id, page_id, file_parent_path, DMFile::ReadMetaMode::all());
                auto column_file = std::make_shared<ColumnFileBig>(*dm_context, ref_file, segment_range);
                if (column_file->getRows() != 0)
                {
                    column_files.emplace_back(std::move(column_file));
                    wbs.data.putRefPage(page_id, file->pageId());
                }
            }

            // We have to commit those file_ids to PageStorage, because as soon as packs are written into segments,
            // they are visible for readers who require file_ids to be found in PageStorage.
            wbs.writeLogAndData();

            bool ingest_success = segment->ingestColumnFiles(*dm_context, range.shrink(segment_range), column_files, clear_data_in_range);
            fiu_do_on(FailPoints::force_set_segment_ingest_packs_fail, { ingest_success = false; });
            if (ingest_success)
            {
                updated_segments.push_back(segment);
                fiu_do_on(FailPoints::segment_merge_after_ingest_packs, {
                    segment->flushCache(*dm_context);
                    segmentMergeDelta(*dm_context, segment, TaskRunThread::BackgroundThreadPool);
                    storage_pool->gc(global_context.getSettingsRef(), StoragePool::Seconds(0));
                });
                break;
            }
            else
            {
                wbs.rollbackWrittenLogAndData();
            }
        }

        cur_range.setStart(segment_range.end);
        cur_range.setEnd(range.end);
    }

    // Enable gc for DTFile after all segment applied.
    // Note that we can not enable gc for them once they have applied to any segments.
    // Assume that one segment get compacted after file ingested, `gc_handle` gc the
    // DTFiles before they get applied to all segments. Then we will apply some
    // deleted DTFiles to other segments.
    for (const auto & file : files)
        file->enableGC();
    // After the ingest DTFiles applied, remove the original page
    ingest_wbs.rollbackWrittenLogAndData();

    {
        // Add some logging about the ingested file ids and updated segments
        // Example: "ingest dmf_1001,1002,1003 into segment [1,3]"
        //          "ingest <empty> into segment [1,3]"
        FmtBuffer fmt_buf;
        if (file_ids.empty())
        {
            fmt_buf.append("ingest <empty>");
        }
        else
        {
            fmt_buf.append("ingest dmf_");
            fmt_buf.joinStr(
                file_ids.begin(),
                file_ids.end(),
                [](const PageId id, FmtBuffer & fb) { fb.fmtAppend("{}", id); },
                ",");
        }
        fmt_buf.append(" into segment [");
        fmt_buf.joinStr(
            updated_segments.begin(),
            updated_segments.end(),
            [](const auto & segment, FmtBuffer & fb) { fb.fmtAppend("{}", segment->segmentId()); },
            ",");
        fmt_buf.append("]");
        LOG_FMT_INFO(
            log,
            "table: {}.{}, clear_data: {}, {}",
            db_name,
            table_name,
            clear_data_in_range,
            fmt_buf.toString());
    }

    GET_METRIC(tiflash_storage_throughput_bytes, type_ingest).Increment(bytes);
    GET_METRIC(tiflash_storage_throughput_rows, type_ingest).Increment(rows);

    flushCache(dm_context, range);

    // TODO: Update the tracing_id before checkSegmentUpdate?
    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);
}

void DeltaMergeStore::deleteRange(const Context & db_context, const DB::Settings & db_settings, const RowKeyRange & delete_range)
{
    LOG_FMT_INFO(log, "table: {}.{} delete range {}", db_name, table_name, delete_range.toDebugString());

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

void DeltaMergeStore::flushCache(const DMContextPtr & dm_context, const RowKeyRange & range)
{
    size_t sleep_ms = 5;

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

            if (segment->flushCache(*dm_context))
            {
                break;
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
}

void DeltaMergeStore::mergeDeltaAll(const Context & context)
{
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

    for (auto & segment : all_segments)
    {
        segmentMergeDelta(*dm_context, segment, TaskRunThread::Foreground);
    }
}

std::optional<DM::RowKeyRange> DeltaMergeStore::mergeDeltaBySegment(const Context & context, const RowKeyValue & start_key, const TaskRunThread run_thread)
{
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
            const auto new_segment = segmentMergeDelta(*dm_context, segment, run_thread);
            if (new_segment)
            {
                const auto segment_end = new_segment->getRowKeyRange().end;
                if (unlikely(*segment_end.value <= *start_key.value))
                {
                    // The next start key must be > current start key
                    LOG_FMT_ERROR(log, "Assert new_segment.end {} > start {} failed", segment_end.toDebugString(), start_key.toDebugString());
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

// Read data without mvcc filtering && delete-range filtering.
// just for debug
BlockInputStreams DeltaMergeStore::readRaw(const Context & db_context,
                                           const DB::Settings & db_settings,
                                           const ColumnDefines & columns_to_read,
                                           size_t num_streams,
                                           const SegmentIdSet & read_segments,
                                           size_t extra_table_id_index)
{
    SegmentReadTasks tasks;

    auto dm_context = newDMContext(db_context, db_settings, fmt::format("read_raw_{}", db_context.getCurrentQueryId()));

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
            LOG_FMT_WARNING(log, "failpoint force_slow_page_storage_snapshot_release begin");
            std::this_thread::sleep_for(std::chrono::seconds(5 * 60));
            (void)tasks;
            LOG_FMT_WARNING(log, "failpoint force_slow_page_storage_snapshot_release end");
        });
        thread_hold_snapshots.detach();
    });

    auto after_segment_read = [&](const DMContextPtr & dm_context_, const SegmentPtr & segment_) {
        this->checkSegmentUpdate(dm_context_, segment_, ThreadType::Read);
    };
    size_t final_num_stream = std::min(num_streams, tasks.size());
    auto read_task_pool = std::make_shared<SegmentReadTaskPool>(std::move(tasks));

    String req_info;
    if (db_context.getDAGContext() != nullptr && db_context.getDAGContext()->isMPPTask())
        req_info = db_context.getDAGContext()->getMPPTaskId().toString();
    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>(
            dm_context,
            read_task_pool,
            after_segment_read,
            columns_to_read,
            EMPTY_FILTER,
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE,
            true,
            db_settings.dt_raw_filter_range,
            extra_table_id_index,
            physical_table_id,
            req_info);
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
                                        size_t expected_block_size,
                                        const SegmentIdSet & read_segments,
                                        size_t extra_table_id_index)
{
    // Use the id from MPP/Coprocessor level as tracing_id
    auto dm_context = newDMContext(db_context, db_settings, tracing_id);

    SegmentReadTasks tasks = getReadTasksByRanges(*dm_context, sorted_ranges, num_streams, read_segments);

    auto tracing_logger = Logger::get(log->name(), dm_context->tracing_id);
    LOG_FMT_DEBUG(tracing_logger, "Read create segment snapshot done");

    auto after_segment_read = [&](const DMContextPtr & dm_context_, const SegmentPtr & segment_) {
        // TODO: Update the tracing_id before checkSegmentUpdate?
        this->checkSegmentUpdate(dm_context_, segment_, ThreadType::Read);
    };

    GET_METRIC(tiflash_storage_read_tasks_count).Increment(tasks.size());
    size_t final_num_stream = std::max(1, std::min(num_streams, tasks.size()));
    auto read_task_pool = std::make_shared<SegmentReadTaskPool>(std::move(tasks));

    String req_info;
    if (db_context.getDAGContext() != nullptr && db_context.getDAGContext()->isMPPTask())
        req_info = db_context.getDAGContext()->getMPPTaskId().toString();
    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>(
            dm_context,
            read_task_pool,
            after_segment_read,
            columns_to_read,
            filter,
            max_version,
            expected_block_size,
            false,
            db_settings.dt_raw_filter_range,
            extra_table_id_index,
            physical_table_id,
            req_info);
        res.push_back(stream);
    }

    LOG_FMT_DEBUG(tracing_logger, "Read create stream done");

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

    bool should_merge = segment_rows < segment_limit_rows / 3 && segment_bytes < segment_limit_bytes / 3;

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
            delta_last_try_flush_rows = delta_rows;
            delta_last_try_flush_bytes = delta_bytes;
            LOG_FMT_DEBUG(log, "Foreground flush cache {}", segment->info());
            segment->flushCache(*dm_context);
        }
        else if (should_background_flush)
        {
            delta_last_try_flush_rows = delta_rows;
            delta_last_try_flush_bytes = delta_bytes;
            try_add_background_task(BackgroundTask{TaskType::Flush, dm_context, segment, {}});
        }
    }

    // Need to check the latest delta (maybe updated after foreground flush). If it is updating by another thread,
    // give up adding more tasks on this version of delta.
    if (segment->getDelta()->isUpdating())
        return;

    /// Now start trying structure update.

    auto get_merge_sibling = [&]() -> SegmentPtr {
        /// For complexity reason, currently we only try to merge with next segment. Normally it is good enough.

        // The last segment cannot be merged.
        if (segment->getRowKeyRange().isEndInfinite())
            return {};
        SegmentPtr next_segment;
        {
            std::shared_lock read_write_lock(read_write_mutex);

            auto it = segments.find(segment->getRowKeyRange().getEnd());
            // check legality
            if (it == segments.end())
                return {};
            auto & cur_segment = it->second;
            if (cur_segment.get() != segment.get())
                return {};
            ++it;
            if (it == segments.end())
                return {};
            next_segment = it->second;
            auto limit = dm_context->segment_limit_rows / 5;
            if (next_segment->getEstimatedRows() >= limit)
                return {};
        }
        return next_segment;
    };

    auto try_fg_merge_delta = [&]() -> SegmentPtr {
        if (should_foreground_merge_delta_by_rows_or_bytes || should_foreground_merge_delta_by_deletes)
        {
            delta_last_try_merge_delta_rows = delta_rows;

            assert(thread_type == ThreadType::Write);

            Stopwatch watch;
            SCOPE_EXIT({
                if (should_foreground_merge_delta_by_rows_or_bytes)
                    GET_METRIC(tiflash_storage_write_stall_duration_seconds, type_write).Observe(watch.elapsedSeconds());
                if (should_foreground_merge_delta_by_deletes)
                    GET_METRIC(tiflash_storage_write_stall_duration_seconds, type_delete_range).Observe(watch.elapsedSeconds());
            });

            return segmentMergeDelta(*dm_context, segment, TaskRunThread::Foreground);
        }
        return {};
    };
    auto try_bg_merge_delta = [&]() {
        if (should_background_merge_delta)
        {
            delta_last_try_merge_delta_rows = delta_rows;
            try_add_background_task(BackgroundTask{TaskType::MergeDelta, dm_context, segment, {}});
            return true;
        }
        return false;
    };
    auto try_bg_split = [&](const SegmentPtr & seg) {
        if (should_bg_split && !seg->isSplitForbidden())
        {
            delta_last_try_split_rows = delta_rows;
            delta_last_try_split_bytes = delta_bytes;
            try_add_background_task(BackgroundTask{TaskType::Split, dm_context, seg, {}});
            return true;
        }
        return false;
    };
    auto try_fg_split = [&](const SegmentPtr & my_segment) -> bool {
        auto my_segment_size = my_segment->getEstimatedBytes();
        auto my_should_split = my_segment_size >= dm_context->segment_force_split_bytes;
        if (my_should_split && !my_segment->isSplitForbidden())
        {
            return segmentSplit(*dm_context, my_segment, true).first != nullptr;
        }
        return false;
    };
    auto try_bg_merge = [&]() {
        SegmentPtr merge_sibling;
        if (should_merge && (merge_sibling = get_merge_sibling()))
        {
            try_add_background_task(BackgroundTask{TaskType::Merge, dm_context, segment, merge_sibling});
            return true;
        }
        return false;
    };
    auto try_bg_compact = [&]() {
        if (should_compact)
        {
            delta_last_try_compact_column_files = column_file_count;
            try_add_background_task(BackgroundTask{TaskType::Compact, dm_context, segment, {}});
            return true;
        }
        return false;
    };
    auto try_place_delta_index = [&]() {
        if (should_place_delta_index)
        {
            delta_last_try_place_delta_index_rows = delta_rows;
            try_add_background_task(BackgroundTask{TaskType::PlaceIndex, dm_context, segment, {}});
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

    if (try_bg_merge_delta())
        return;
    else if (try_bg_split(segment))
        return;
    else if (try_bg_merge())
        return;
    else if (try_bg_compact())
        return;
    else
        try_place_delta_index();
}

bool DeltaMergeStore::updateGCSafePoint()
{
    if (auto pd_client = global_context.getTMTContext().getPDClient(); !pd_client->isMock())
    {
        auto safe_point = PDClientHelper::getGCSafePointWithRetry(
            pd_client,
            /* ignore_cache= */ false,
            global_context.getSettingsRef().safe_point_update_interval_seconds);
        latest_gc_safe_point.store(safe_point, std::memory_order_release);
        return true;
    }
    return false;
}

bool DeltaMergeStore::handleBackgroundTask(bool heavy)
{
    auto task = background_tasks.nextTask(heavy, log);
    if (!task)
        return false;

    // Update GC safe point before background task
    // Foreground task don't get GC safe point from remote, but we better make it as up to date as possible.
    if (updateGCSafePoint())
    {
        /// Note that `task.dm_context->db_context` will be free after query is finish. We should not use that in background task.
        task.dm_context->min_version = latest_gc_safe_point.load(std::memory_order_relaxed);
        LOG_FMT_DEBUG(log, "Task {} GC safe point: {}", toString(task.type), task.dm_context->min_version);
    }

    SegmentPtr left, right;
    ThreadType type = ThreadType::Write;
    try
    {
        switch (task.type)
        {
        case TaskType::Split:
            std::tie(left, right) = segmentSplit(*task.dm_context, task.segment, false);
            type = ThreadType::BG_Split;
            break;
        case TaskType::Merge:
            segmentMerge(*task.dm_context, task.segment, task.next_segment, false);
            type = ThreadType::BG_Merge;
            break;
        case TaskType::MergeDelta:
        {
            FAIL_POINT_PAUSE(FailPoints::pause_before_dt_background_delta_merge);
            left = segmentMergeDelta(*task.dm_context, task.segment, TaskRunThread::BackgroundThreadPool);
            type = ThreadType::BG_MergeDelta;
            // Wake up all waiting threads if failpoint is enabled
            FailPointHelper::disableFailPoint(FailPoints::pause_until_dt_background_delta_merge);
            break;
        }
        case TaskType::Compact:
            task.segment->compactDelta(*task.dm_context);
            left = task.segment;
            type = ThreadType::BG_Compact;
            break;
        case TaskType::Flush:
            task.segment->flushCache(*task.dm_context);
            // After flush cache, better place delta index.
            task.segment->placeDeltaIndex(*task.dm_context);
            left = task.segment;
            type = ThreadType::BG_Flush;
            break;
        case TaskType::PlaceIndex:
            task.segment->placeDeltaIndex(*task.dm_context);
            break;
        default:
            throw Exception(fmt::format("Unsupported task type: {}", toString(task.type)));
        }
    }
    catch (const Exception & e)
    {
        LOG_FMT_ERROR(
            log,
            "Task {} on Segment [{}]{} failed. Error msg: {}",
            DeltaMergeStore::toString(task.type),
            task.segment->segmentId(),
            ((bool)task.next_segment ? (fmt::format(" and [{}]", task.next_segment->segmentId())) : ""),
            e.message());
        e.rethrow();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    // continue to check whether we need to apply more tasks after this task is ended.
    if (left)
        checkSegmentUpdate(task.dm_context, left, type);
    if (right)
        checkSegmentUpdate(task.dm_context, right, type);

    return true;
}

namespace GC
{
// Returns true if it needs gc.
// This is for optimization purpose, does not mean to be accurate.
bool shouldCompactStable(const SegmentPtr & seg, DB::Timestamp gc_safepoint, double ratio_threshold, const LoggerPtr & log)
{
    // Always GC.
    if (ratio_threshold < 1.0)
        return true;

    const auto & property = seg->getStable()->getStableProperty();
    LOG_FMT_TRACE(log, "{}", property.toDebugString());
    // No data older than safe_point to GC.
    if (property.gc_hint_version > gc_safepoint)
        return false;
    // A lot of MVCC versions to GC.
    if (property.num_versions > property.num_rows * ratio_threshold)
        return true;
    // A lot of non-effective MVCC versions to GC.
    if (property.num_versions > property.num_puts * ratio_threshold)
        return true;
    return false;
}

bool shouldCompactDeltaWithStable(const DMContext & context, const SegmentSnapshotPtr & snap, const RowKeyRange & segment_range, double ratio_threshold, const LoggerPtr & log)
{
    auto actual_delete_range = snap->delta->getSquashDeleteRange().shrink(segment_range);
    if (actual_delete_range.none())
        return false;

    auto [delete_rows, delete_bytes] = snap->stable->getApproxRowsAndBytes(context, actual_delete_range);

    auto stable_rows = snap->stable->getRows();
    auto stable_bytes = snap->stable->getBytes();

    LOG_FMT_TRACE(log, "delete range rows [{}], delete_bytes [{}] stable_rows [{}] stable_bytes [{}]", delete_rows, delete_bytes, stable_rows, stable_bytes);

    // 1. for small tables, the data may just reside in delta and stable_rows may be 0,
    //   so the `=` in `>=` is needed to cover the scenario when set tiflash replica of small tables to 0.
    //   (i.e. `actual_delete_range` is not none, but `delete_rows` and `stable_rows` are both 0).
    // 2. the disadvantage of `=` in `>=` is that it may trigger an extra gc when write apply snapshot file to an empty segment,
    //   because before write apply snapshot file, it will write a delete range first, and will meet the following gc criteria.
    //   But the cost should be really minor because merge delta on an empty segment should be very fast.
    //   What's more, we can ignore this kind of delete range in future to avoid this extra gc.
    bool should_compact = (delete_rows >= stable_rows * ratio_threshold) || (delete_bytes >= stable_bytes * ratio_threshold);
    return should_compact;
}
} // namespace GC

UInt64 DeltaMergeStore::onSyncGc(Int64 limit)
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return 0;

    if (!updateGCSafePoint())
        return 0;

    {
        std::shared_lock lock(read_write_mutex);
        // avoid gc on empty tables
        if (segments.size() == 1)
        {
            const auto & seg = segments.begin()->second;
            if (seg->getEstimatedRows() == 0)
                return 0;
        }
    }

    DB::Timestamp gc_safe_point = latest_gc_safe_point.load(std::memory_order_acquire);
    LOG_FMT_DEBUG(log,
                  "GC on table {} start with key: {}, gc_safe_point: {}, max gc limit: {}",
                  table_name,
                  next_gc_check_key.toDebugString(),
                  gc_safe_point,
                  limit);

    UInt64 check_segments_num = 0;
    Int64 gc_segments_num = 0;
    while (gc_segments_num < limit)
    {
        // If the store is shut down, give up running GC on it.
        if (shutdown_called.load(std::memory_order_relaxed))
            break;

        auto dm_context = newDMContext(global_context, global_context.getSettingsRef(), "onSyncGc");
        SegmentPtr segment;
        SegmentSnapshotPtr segment_snap;
        {
            std::shared_lock lock(read_write_mutex);

            auto segment_it = segments.upper_bound(next_gc_check_key.toRowKeyValueRef());
            if (segment_it == segments.end())
                segment_it = segments.begin();

            // we have check all segments, stop here
            if (check_segments_num >= segments.size())
                break;
            check_segments_num++;

            segment = segment_it->second;
            next_gc_check_key = segment_it->first.toRowKeyValue();
            segment_snap = segment->createSnapshot(*dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfDeltaMerge);
        }

        assert(segment != nullptr);
        if (segment->hasAbandoned() || segment_snap == nullptr)
            continue;

        const auto segment_id = segment->segmentId();
        RowKeyRange segment_range = segment->getRowKeyRange();

        // meet empty segment, try merge it
        if (segment_snap->getRows() == 0)
        {
            // release segment_snap before checkSegmentUpdate, otherwise this segment is still in update status.
            segment_snap = {};
            checkSegmentUpdate(dm_context, segment, ThreadType::BG_GC);
            continue;
        }

        try
        {
            // Check whether we should apply gc on this segment
            bool should_compact = false;
            if (GC::shouldCompactDeltaWithStable(
                    *dm_context,
                    segment_snap,
                    segment_range,
                    global_context.getSettingsRef().dt_bg_gc_delta_delete_ratio_to_trigger_gc,
                    log))
            {
                should_compact = true;
            }
            else if (segment->getLastCheckGCSafePoint() < gc_safe_point)
            {
                // Avoid recheck this segment when gc_safe_point doesn't change regardless whether we trigger this segment's DeltaMerge or not.
                // Because after we calculate StableProperty and compare it with this gc_safe_point,
                // there is no need to recheck it again using the same gc_safe_point.
                // On the other hand, if it should do DeltaMerge using this gc_safe_point, and the DeltaMerge is interruptted by other process,
                // it's still worth to wait another gc_safe_point to check this segment again.
                segment->setLastCheckGCSafePoint(gc_safe_point);
                dm_context->min_version = gc_safe_point;

                // calculate StableProperty if needed
                if (!segment->getStable()->isStablePropertyCached())
                    segment->getStable()->calculateStableProperty(*dm_context, segment_range, isCommonHandle());

                should_compact = GC::shouldCompactStable(
                    segment,
                    gc_safe_point,
                    global_context.getSettingsRef().dt_bg_gc_ratio_threhold_to_trigger_gc,
                    log);
            }
            bool finish_gc_on_segment = false;
            if (should_compact)
            {
                if (segment = segmentMergeDelta(*dm_context, segment, TaskRunThread::BackgroundGCThread, segment_snap); segment)
                {
                    // Continue to check whether we need to apply more tasks on this segment
                    segment_snap = {};
                    checkSegmentUpdate(dm_context, segment, ThreadType::BG_GC);
                    gc_segments_num++;
                    finish_gc_on_segment = true;
                    LOG_FMT_INFO(
                        log,
                        "GC-merge-delta done on Segment [{}] [range={}] [table={}]",
                        segment_id,
                        segment_range.toDebugString(),
                        table_name);
                }
                else
                {
                    LOG_FMT_INFO(
                        log,
                        "GC aborted on Segment [{}] [range={}] [table={}]",
                        segment_id,
                        segment_range.toDebugString(),
                        table_name);
                }
            }
            if (!finish_gc_on_segment)
                LOG_FMT_TRACE(
                    log,
                    "GC is skipped Segment [{}] [range={}] [table={}]",
                    segment_id,
                    segment_range.toDebugString(),
                    table_name);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("while apply gc Segment [{}] [range={}] [table={}]", segment_id, segment_range.toDebugString(), table_name));
            e.rethrow();
        }
    }

    LOG_FMT_DEBUG(log, "Finish GC on {} segments [table={}]", gc_segments_num, table_name);
    return gc_segments_num;
}

SegmentPair DeltaMergeStore::segmentSplit(DMContext & dm_context, const SegmentPtr & segment, bool is_foreground)
{
    LOG_FMT_DEBUG(
        log,
        "{} split segment {}, safe point: {}",
        (is_foreground ? "Foreground" : "Background"),
        segment->info(),
        dm_context.min_version);

    SegmentSnapshotPtr segment_snap;
    ColumnDefinesPtr schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
        {
            LOG_FMT_DEBUG(log, "Give up segment [{}] split", segment->segmentId());
            return {};
        }

        segment_snap = segment->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentSplit);
        if (!segment_snap || !segment_snap->getRows())
        {
            LOG_FMT_DEBUG(log, "Give up segment [{}] split", segment->segmentId());
            return {};
        }
        schema_snap = store_columns;
    }

    // Not counting the early give up action.
    auto delta_bytes = static_cast<Int64>(segment_snap->delta->getBytes());
    auto delta_rows = static_cast<Int64>(segment_snap->delta->getRows());

    size_t duplicated_bytes = 0;
    size_t duplicated_rows = 0;

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_SegmentSplit};
    if (is_foreground)
        GET_METRIC(tiflash_storage_subtask_count, type_seg_split_fg).Increment();
    else
        GET_METRIC(tiflash_storage_subtask_count, type_seg_split_bg).Increment();

    Stopwatch watch_seg_split;
    SCOPE_EXIT({
        if (is_foreground)
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_seg_split_fg).Observe(watch_seg_split.elapsedSeconds());
        else
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_seg_split_bg).Observe(watch_seg_split.elapsedSeconds());
    });

    WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());

    auto range = segment->getRowKeyRange();
    auto split_info_opt = segment->prepareSplit(dm_context, schema_snap, segment_snap, wbs);

    if (!split_info_opt.has_value())
    {
        // Likely we can not find an appropriate split point for this segment later, forbid the split until this segment get updated through applying delta-merge. Or it will slow down the write a lot.
        segment->forbidSplit();
        LOG_FMT_WARNING(log, "Giving up and forbid later split. Segment [{}]. Because of prepare split failed", segment->segmentId());
        return {};
    }

    auto & split_info = split_info_opt.value();

    wbs.writeLogAndData();
    split_info.my_stable->enableDMFilesGC();
    split_info.other_stable->enableDMFilesGC();

    SegmentPtr new_left, new_right;
    {
        std::unique_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
        {
            LOG_FMT_DEBUG(log, "Give up segment [{}] split", segment->segmentId());
            wbs.setRollback();
            return {};
        }

        LOG_FMT_DEBUG(log, "Apply split. Segment [{}]", segment->segmentId());

        auto segment_lock = segment->mustGetUpdateLock();

        std::tie(new_left, new_right) = segment->applySplit(dm_context, segment_snap, wbs, split_info);

        wbs.writeMeta();

        segment->abandon(dm_context);
        segments.erase(range.getEnd());
        id_to_segment.erase(segment->segmentId());

        segments[new_left->getRowKeyRange().getEnd()] = new_left;
        segments[new_right->getRowKeyRange().getEnd()] = new_right;

        id_to_segment.emplace(new_left->segmentId(), new_left);
        id_to_segment.emplace(new_right->segmentId(), new_right);

        if constexpr (DM_RUN_CHECK)
        {
            new_left->check(dm_context, "After split left");
            new_right->check(dm_context, "After split right");
        }

        duplicated_bytes = new_left->getDelta()->getBytes();
        duplicated_rows = new_right->getDelta()->getBytes();

        LOG_FMT_DEBUG(log, "Apply split done. Segment [{}]", segment->segmentId());
    }

    wbs.writeRemoves();

    if (!split_info.is_logical)
    {
        GET_METRIC(tiflash_storage_throughput_bytes, type_split).Increment(delta_bytes);
        GET_METRIC(tiflash_storage_throughput_rows, type_split).Increment(delta_rows);
    }
    else
    {
        // For logical split, delta is duplicated into two segments. And will be merged into stable twice later. So we need to decrease it here.
        // Otherwise the final total delta merge bytes is greater than bytes written into.
        GET_METRIC(tiflash_storage_throughput_bytes, type_split).Decrement(duplicated_bytes);
        GET_METRIC(tiflash_storage_throughput_rows, type_split).Decrement(duplicated_rows);
    }

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);

    return {new_left, new_right};
}

void DeltaMergeStore::segmentMerge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right, bool is_foreground)
{
    LOG_FMT_DEBUG(
        log,
        "{} merge Segment [{}] and [{}], safe point: {}",
        (is_foreground ? "Foreground" : "Background"),
        left->info(),
        right->info(),
        dm_context.min_version);

    /// This segment may contain some rows that not belong to this segment range which is left by previous split operation.
    /// And only saved data in this segment will be filtered by the segment range in the merge process,
    /// unsaved data will be directly copied to the new segment.
    /// So we flush here to make sure that all potential data left by previous split operation is saved.
    while (!left->flushCache(dm_context))
    {
        // keep flush until success if not abandoned
        if (left->hasAbandoned())
        {
            LOG_FMT_DEBUG(log, "Give up merge segments left [{}], right [{}]", left->segmentId(), right->segmentId());
            return;
        }
    }
    while (!right->flushCache(dm_context))
    {
        // keep flush until success if not abandoned
        if (right->hasAbandoned())
        {
            LOG_FMT_DEBUG(log, "Give up merge segments left [{}], right [{}]", left->segmentId(), right->segmentId());
            return;
        }
    }

    SegmentSnapshotPtr left_snap;
    SegmentSnapshotPtr right_snap;
    ColumnDefinesPtr schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, left))
        {
            LOG_FMT_DEBUG(log, "Give up merge segments left [{}], right [{}]", left->segmentId(), right->segmentId());
            return;
        }
        if (!isSegmentValid(lock, right))
        {
            LOG_FMT_DEBUG(log, "Give up merge segments left [{}], right [{}]", left->segmentId(), right->segmentId());
            return;
        }

        left_snap = left->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
        right_snap = right->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentMerge);

        if (!left_snap || !right_snap)
        {
            LOG_FMT_DEBUG(log, "Give up merge segments left [{}], right [{}]", left->segmentId(), right->segmentId());
            return;
        }
        schema_snap = store_columns;
    }

    // Not counting the early give up action.
    auto delta_bytes = static_cast<Int64>(left_snap->delta->getBytes()) + right_snap->getBytes();
    auto delta_rows = static_cast<Int64>(left_snap->delta->getRows()) + right_snap->getRows();

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_SegmentMerge};
    if (!is_foreground)
        GET_METRIC(tiflash_storage_subtask_count, type_seg_merge_bg_gc).Increment();
    Stopwatch watch_seg_merge;
    SCOPE_EXIT({
        if (!is_foreground)
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_seg_merge_bg_gc).Observe(watch_seg_merge.elapsedSeconds());
    });

    auto left_range = left->getRowKeyRange();
    auto right_range = right->getRowKeyRange();

    WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());
    auto merged_stable = Segment::prepareMerge(dm_context, schema_snap, left, left_snap, right, right_snap, wbs);
    wbs.writeLogAndData();
    merged_stable->enableDMFilesGC();

    {
        std::unique_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, left) || !isSegmentValid(lock, right))
        {
            LOG_FMT_DEBUG(log, "Give up merge segments left [{}], right [{}]", left->segmentId(), right->segmentId());
            wbs.setRollback();
            return;
        }

        LOG_FMT_DEBUG(log, "Apply merge. Left [{}], right [{}]", left->segmentId(), right->segmentId());

        auto left_lock = left->mustGetUpdateLock();
        auto right_lock = right->mustGetUpdateLock();

        auto merged = Segment::applyMerge(dm_context, left, left_snap, right, right_snap, wbs, merged_stable);

        wbs.writeMeta();

        left->abandon(dm_context);
        right->abandon(dm_context);
        segments.erase(left_range.getEnd());
        segments.erase(right_range.getEnd());
        id_to_segment.erase(left->segmentId());
        id_to_segment.erase(right->segmentId());

        segments.emplace(merged->getRowKeyRange().getEnd(), merged);
        id_to_segment.emplace(merged->segmentId(), merged);

        if constexpr (DM_RUN_CHECK)
        {
            merged->check(dm_context, "After segment merge");
        }

        LOG_FMT_DEBUG(log, "Apply merge done. [{}] and [{}]", left->info(), right->info());
    }

    wbs.writeRemoves();

    GET_METRIC(tiflash_storage_throughput_bytes, type_merge).Increment(delta_bytes);
    GET_METRIC(tiflash_storage_throughput_rows, type_merge).Increment(delta_rows);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);
}

SegmentPtr DeltaMergeStore::segmentMergeDelta(
    DMContext & dm_context,
    const SegmentPtr & segment,
    const TaskRunThread run_thread,
    SegmentSnapshotPtr segment_snap)
{
    LOG_FMT_DEBUG(log, "{} merge delta, segment [{}], safe point: {}", toString(run_thread), segment->segmentId(), dm_context.min_version);

    ColumnDefinesPtr schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
        {
            LOG_FMT_DEBUG(log, "Give up merge delta, segment [{}]", segment->segmentId());
            return {};
        }

        // Try to generate a new snapshot if there is no pre-allocated one
        if (!segment_snap)
            segment_snap = segment->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfDeltaMerge);

        if (unlikely(!segment_snap))
        {
            LOG_FMT_DEBUG(log, "Give up merge delta, segment [{}]", segment->segmentId());
            return {};
        }
        schema_snap = store_columns;
    }

    // Not counting the early give up action.
    auto delta_bytes = static_cast<Int64>(segment_snap->delta->getBytes());
    auto delta_rows = static_cast<Int64>(segment_snap->delta->getRows());

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_DeltaMerge};
    CurrentMetrics::Increment cur_dm_total_bytes{CurrentMetrics::DT_DeltaMergeTotalBytes, static_cast<Int64>(segment_snap->getBytes())};
    CurrentMetrics::Increment cur_dm_total_rows{CurrentMetrics::DT_DeltaMergeTotalRows, static_cast<Int64>(segment_snap->getRows())};

    switch (run_thread)
    {
    case TaskRunThread::BackgroundThreadPool:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge_bg).Increment();
        break;
    case TaskRunThread::Foreground:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge_fg).Increment();
        break;
    case TaskRunThread::ForegroundRPC:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge_manual).Increment();
        break;
    case TaskRunThread::BackgroundGCThread:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge_bg_gc).Increment();
        break;
    default:
        break;
    }

    Stopwatch watch_delta_merge;
    SCOPE_EXIT({
        switch (run_thread)
        {
        case TaskRunThread::BackgroundThreadPool:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_bg).Observe(watch_delta_merge.elapsedSeconds());
            break;
        case TaskRunThread::Foreground:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_fg).Observe(watch_delta_merge.elapsedSeconds());
            break;
        case TaskRunThread::ForegroundRPC:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_manual).Observe(watch_delta_merge.elapsedSeconds());
            break;
        case TaskRunThread::BackgroundGCThread:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_bg_gc).Observe(watch_delta_merge.elapsedSeconds());
            break;
        default:
            break;
        }
    });

    WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());

    auto new_stable = segment->prepareMergeDelta(dm_context, schema_snap, segment_snap, wbs);
    wbs.writeLogAndData();
    new_stable->enableDMFilesGC();

    SegmentPtr new_segment;
    {
        std::unique_lock read_write_lock(read_write_mutex);

        if (!isSegmentValid(read_write_lock, segment))
        {
            LOG_FMT_DEBUG(log, "Give up merge delta, segment [{}]", segment->segmentId());
            wbs.setRollback();
            return {};
        }

        LOG_FMT_DEBUG(log, "Apply merge delta. Segment [{}]", segment->info());

        auto segment_lock = segment->mustGetUpdateLock();

        new_segment = segment->applyMergeDelta(dm_context, segment_snap, wbs, new_stable);

        wbs.writeMeta();


        // The instance of PKRange::End is closely linked to instance of PKRange. So we cannot reuse it.
        // Replace must be done by erase + insert.
        segments.erase(segment->getRowKeyRange().getEnd());
        id_to_segment.erase(segment->segmentId());

        segments[new_segment->getRowKeyRange().getEnd()] = new_segment;
        id_to_segment[new_segment->segmentId()] = new_segment;

        segment->abandon(dm_context);

        if constexpr (DM_RUN_CHECK)
        {
            new_segment->check(dm_context, "After merge delta");
        }

        LOG_FMT_DEBUG(log, "Apply merge delta done. Segment [{}]", segment->segmentId());
    }

    wbs.writeRemoves();

    GET_METRIC(tiflash_storage_throughput_bytes, type_delta_merge).Increment(delta_bytes);
    GET_METRIC(tiflash_storage_throughput_rows, type_delta_merge).Increment(delta_rows);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);

    return new_segment;
}

bool DeltaMergeStore::doIsSegmentValid(const SegmentPtr & segment)
{
    if (segment->hasAbandoned())
    {
        LOG_FMT_DEBUG(log, "Segment [{}] instance has abandoned", segment->segmentId());
        return false;
    }
    // Segment instance could have been removed or replaced.
    auto it = segments.find(segment->getRowKeyRange().getEnd());
    if (it == segments.end())
    {
        LOG_FMT_DEBUG(log, "Segment [{}] not found in segment map", segment->segmentId());

        auto it2 = id_to_segment.find(segment->segmentId());
        if (it2 != id_to_segment.end())
        {
            LOG_FMT_DEBUG(
                log,
                "Found segment with same id in id_to_segment: {}, while my segment: {}",
                it2->second->info(),
                segment->info());
        }
        return false;
    }
    auto & cur_segment = it->second;
    if (cur_segment.get() != segment.get())
    {
        LOG_FMT_DEBUG(log, "Segment [{}] instance has been replaced in segment map", segment->segmentId());
        return false;
    }
    return true;
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
    LOG_FMT_DEBUG(log, "Loading dt files");

    DMFile::ListOptions options;
    options.only_list_can_gc = false; // We need all files to restore the bytes on disk
    options.clean_up = true;
    auto file_provider = global_context.getFileProvider();
    auto path_delegate = path_pool.getStableDiskDelegator();
    for (const auto & root_path : path_delegate.listPaths())
    {
        for (const auto & file_id : DMFile::listAllInPath(file_provider, root_path, options))
        {
            auto dmfile = DMFile::restore(file_provider, file_id, /* page_id= */ 0, root_path, DMFile::ReadMetaMode::diskSizeOnly());
            path_delegate.addDTFile(file_id, dmfile->getBytesOnDisk(), root_path);
        }
    }
}

static inline DB::PS::V2::PageEntriesVersionSetWithDelta::Snapshot *
toConcreteSnapshot(const DB::PageStorage::SnapshotPtr & ptr)
{
    return dynamic_cast<DB::PS::V2::PageEntriesVersionSetWithDelta::Snapshot *>(ptr.get());
}

DeltaMergeStoreStat DeltaMergeStore::getStat()
{
    std::shared_lock lock(read_write_mutex);

    DeltaMergeStoreStat stat;

    if (shutdown_called.load(std::memory_order_relaxed))
        return stat;

    stat.segment_count = segments.size();

    Int64 total_placed_rows = 0;
    Int64 total_delta_cache_rows = 0;
    Float64 total_delta_cache_size = 0;
    Int64 total_delta_valid_cache_rows = 0;
    for (const auto & [handle, segment] : segments)
    {
        (void)handle;
        const auto & delta = segment->getDelta();
        const auto & stable = segment->getStable();

        total_placed_rows += delta->getPlacedDeltaRows();

        if (delta->getColumnFileCount())
        {
            stat.total_rows += delta->getRows();
            stat.total_size += delta->getBytes();

            stat.total_delete_ranges += delta->getDeletes();

            stat.delta_count += 1;
            stat.total_pack_count_in_delta += delta->getColumnFileCount();

            stat.total_delta_rows += delta->getRows();
            stat.total_delta_size += delta->getBytes();

            stat.delta_index_size += delta->getDeltaIndexBytes();

            total_delta_cache_rows += delta->getTotalCacheRows();
            total_delta_cache_size += delta->getTotalCacheBytes();
            total_delta_valid_cache_rows += delta->getValidCacheRows();
        }

        if (stable->getPacks())
        {
            stat.total_rows += stable->getRows();
            stat.total_size += stable->getBytes();

            stat.stable_count += 1;
            stat.total_pack_count_in_stable += stable->getPacks();

            stat.total_stable_rows += stable->getRows();
            stat.total_stable_size += stable->getBytes();
            stat.total_stable_size_on_disk += stable->getBytesOnDisk();
        }
    }

    stat.delta_rate_rows = static_cast<Float64>(stat.total_delta_rows) / stat.total_rows;
    stat.delta_rate_segments = static_cast<Float64>(stat.delta_count) / stat.segment_count;

    stat.delta_placed_rate = static_cast<Float64>(total_placed_rows) / stat.total_delta_rows;
    stat.delta_cache_size = total_delta_cache_size;
    stat.delta_cache_rate = static_cast<Float64>(total_delta_valid_cache_rows) / stat.total_delta_rows;
    stat.delta_cache_wasted_rate = static_cast<Float64>(total_delta_cache_rows - total_delta_valid_cache_rows) / total_delta_valid_cache_rows;

    stat.avg_segment_rows = static_cast<Float64>(stat.total_rows) / stat.segment_count;
    stat.avg_segment_size = static_cast<Float64>(stat.total_size) / stat.segment_count;

    stat.avg_delta_rows = static_cast<Float64>(stat.total_delta_rows) / stat.delta_count;
    stat.avg_delta_size = static_cast<Float64>(stat.total_delta_size) / stat.delta_count;
    stat.avg_delta_delete_ranges = static_cast<Float64>(stat.total_delete_ranges) / stat.delta_count;

    stat.avg_stable_rows = static_cast<Float64>(stat.total_stable_rows) / stat.stable_count;
    stat.avg_stable_size = static_cast<Float64>(stat.total_stable_size) / stat.stable_count;

    stat.avg_pack_count_in_delta = static_cast<Float64>(stat.total_pack_count_in_delta) / stat.delta_count;
    stat.avg_pack_rows_in_delta = static_cast<Float64>(stat.total_delta_rows) / stat.total_pack_count_in_delta;
    stat.avg_pack_size_in_delta = static_cast<Float64>(stat.total_delta_size) / stat.total_pack_count_in_delta;

    stat.avg_pack_count_in_stable = static_cast<Float64>(stat.total_pack_count_in_stable) / stat.stable_count;
    stat.avg_pack_rows_in_stable = static_cast<Float64>(stat.total_stable_rows) / stat.total_pack_count_in_stable;
    stat.avg_pack_size_in_stable = static_cast<Float64>(stat.total_stable_size) / stat.total_pack_count_in_stable;

    static const String useless_tracing_id("DeltaMergeStore::getStat");
    {
        auto snaps_stat = storage_pool->dataReader()->getSnapshotsStat();
        stat.storage_stable_num_snapshots = snaps_stat.num_snapshots;
        stat.storage_stable_oldest_snapshot_lifetime = snaps_stat.longest_living_seconds;
        stat.storage_stable_oldest_snapshot_thread_id = snaps_stat.longest_living_from_thread_id;
        stat.storage_stable_oldest_snapshot_tracing_id = snaps_stat.longest_living_from_tracing_id;
        PageStorage::SnapshotPtr stable_snapshot = storage_pool->dataReader()->getSnapshot(useless_tracing_id);
        const auto * concrete_snap = toConcreteSnapshot(stable_snapshot);
        if (concrete_snap)
        {
            if (const auto * const version = concrete_snap->version(); version != nullptr)
            {
                stat.storage_stable_num_pages = version->numPages();
                stat.storage_stable_num_normal_pages = version->numNormalPages();
                stat.storage_stable_max_page_id = version->maxId();
            }
            else
            {
                LOG_FMT_ERROR(log, "Can't get any version from current snapshot.[type=data] [database={}] [table={}]", db_name, table_name);
            }
        }
    }
    {
        auto snaps_stat = storage_pool->logReader()->getSnapshotsStat();
        stat.storage_delta_num_snapshots = snaps_stat.num_snapshots;
        stat.storage_delta_oldest_snapshot_lifetime = snaps_stat.longest_living_seconds;
        stat.storage_delta_oldest_snapshot_thread_id = snaps_stat.longest_living_from_thread_id;
        stat.storage_delta_oldest_snapshot_tracing_id = snaps_stat.longest_living_from_tracing_id;
        PageStorage::SnapshotPtr log_snapshot = storage_pool->logReader()->getSnapshot(useless_tracing_id);
        const auto * concrete_snap = toConcreteSnapshot(log_snapshot);
        if (concrete_snap)
        {
            if (const auto * const version = concrete_snap->version(); version != nullptr)
            {
                stat.storage_delta_num_pages = version->numPages();
                stat.storage_delta_num_normal_pages = version->numNormalPages();
                stat.storage_delta_max_page_id = version->maxId();
            }
            else
            {
                LOG_FMT_ERROR(log, "Can't get any version from current snapshot.[type=log] [database={}] [table={}]", db_name, table_name);
            }
        }
    }
    {
        auto snaps_stat = storage_pool->metaReader()->getSnapshotsStat();
        stat.storage_meta_num_snapshots = snaps_stat.num_snapshots;
        stat.storage_meta_oldest_snapshot_lifetime = snaps_stat.longest_living_seconds;
        stat.storage_meta_oldest_snapshot_thread_id = snaps_stat.longest_living_from_thread_id;
        stat.storage_meta_oldest_snapshot_tracing_id = snaps_stat.longest_living_from_tracing_id;
        PageStorage::SnapshotPtr meta_snapshot = storage_pool->metaReader()->getSnapshot(useless_tracing_id);
        const auto * concrete_snap = toConcreteSnapshot(meta_snapshot);
        if (concrete_snap)
        {
            if (const auto * const version = concrete_snap->version(); version != nullptr)
            {
                stat.storage_meta_num_pages = version->numPages();
                stat.storage_meta_num_normal_pages = version->numNormalPages();
                stat.storage_meta_max_page_id = version->maxId();
            }
            else
            {
                LOG_FMT_ERROR(log, "Can't get any version from current snapshot.[type=meta] [database={}] [table={}]", db_name, table_name);
            }
        }
    }

    stat.background_tasks_length = background_tasks.length();

    return stat;
}

SegmentStats DeltaMergeStore::getSegmentStats()
{
    std::shared_lock lock(read_write_mutex);

    SegmentStats stats;
    for (const auto & [handle, segment] : segments)
    {
        (void)handle;

        SegmentStat stat;
        const auto & delta = segment->getDelta();
        const auto & stable = segment->getStable();

        stat.segment_id = segment->segmentId();
        stat.range = segment->getRowKeyRange();

        stat.rows = segment->getEstimatedRows();
        stat.size = delta->getBytes() + stable->getBytes();
        stat.delete_ranges = delta->getDeletes();

        stat.stable_size_on_disk = stable->getBytesOnDisk();

        stat.delta_pack_count = delta->getColumnFileCount();
        stat.stable_pack_count = stable->getPacks();

        stat.avg_delta_pack_rows = static_cast<Float64>(delta->getRows()) / stat.delta_pack_count;
        stat.avg_stable_pack_rows = static_cast<Float64>(stable->getRows()) / stat.stable_pack_count;

        stat.delta_rate = static_cast<Float64>(delta->getRows()) / stat.rows;
        stat.delta_cache_size = delta->getTotalCacheBytes();

        stat.delta_index_size = delta->getDeltaIndexBytes();

        stats.push_back(stat);
    }
    return stats;
}

SegmentReadTasks DeltaMergeStore::getReadTasksByRanges(
    DMContext & dm_context,
    const RowKeyRanges & sorted_ranges,
    size_t expected_tasks_count,
    const SegmentIdSet & read_segments)
{
    SegmentReadTasks tasks;

    std::shared_lock lock(read_write_mutex);

    auto range_it = sorted_ranges.begin();
    auto seg_it = segments.upper_bound(range_it->getStart());

    if (seg_it == segments.end())
    {
        throw Exception(
            fmt::format("Failed to locate segment begin with start in range: {}", range_it->toDebugString()),
            ErrorCodes::LOGICAL_ERROR);
    }

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
                if (unlikely(!segment_snap))
                    throw Exception("Failed to get segment snap", ErrorCodes::LOGICAL_ERROR);
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

    /// Try to make task number larger or equal to expected_tasks_count.
    auto result_tasks = SegmentReadTask::trySplitReadTasks(tasks, expected_tasks_count);

    size_t total_ranges = 0;
    for (auto & task : result_tasks)
    {
        /// Merge continuously ranges.
        task->mergeRanges();
        total_ranges += task->ranges.size();
    }

    auto tracing_logger = Logger::get(log->name(), dm_context.tracing_id);
    LOG_FMT_DEBUG(
        tracing_logger,
        "[sorted_ranges: {}] [tasks before split: {}] [tasks final: {}] [ranges final: {}]",
        sorted_ranges.size(),
        tasks.size(),
        result_tasks.size(),
        total_ranges);

    return result_tasks;
}

} // namespace DM
} // namespace DB

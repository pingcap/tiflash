#include <Columns/ColumnVector.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/typeid_cast.h>
#include <Core/SortDescription.h>
#include <Functions/FunctionsConversion.h>
#include <Interpreters/sortBlock.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMSegmentThreadInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/SchemaUpdate.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/DeltaMerge/StableValueSpace.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/TMTContext.h>

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
} // namespace FailPoints

namespace DM
{

// ================================================
//   MergeDeltaTaskPool
// ================================================

bool DeltaMergeStore::MergeDeltaTaskPool::addTask(const BackgroundTask & task, const ThreadType & whom, Logger * log_)
{
    LOG_DEBUG(log_,
              "Segment [" << task.segment->segmentId() << "] task [" << toString(task.type) << "] add to background task pool by ["
                          << toString(whom) << "]");

    std::scoped_lock lock(mutex);
    switch (task.type)
    {
    case TaskType::Split:
    case TaskType::Merge:
    case TaskType::MergeDelta:
        heavy_tasks.push(task);
        return true;
    case TaskType::Compact:
    case TaskType::Flush:
    case TaskType::PlaceIndex:
        light_tasks.push(task);
        return false;
    default:
        throw Exception("Unsupported task type: " + DeltaMergeStore::toString(task.type));
    }
}

DeltaMergeStore::BackgroundTask DeltaMergeStore::MergeDeltaTaskPool::nextTask(bool is_heavy, Logger * log_)
{
    std::scoped_lock lock(mutex);

    auto & tasks = is_heavy ? heavy_tasks : light_tasks;
    if (tasks.empty())
        return {};
    auto task = tasks.front();
    tasks.pop();

    LOG_DEBUG(log_, "Segment [" << task.segment->segmentId() << "] task [" << toString(task.type) << "] pop from background task pool");

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

DeltaMergeStore::DeltaMergeStore(Context &             db_context,
                                 bool                  data_path_contains_database_name,
                                 const String &        db_name_,
                                 const String &        table_name_,
                                 const ColumnDefines & columns,
                                 const ColumnDefine &  handle,
                                 bool                  is_common_handle_,
                                 size_t                rowkey_column_size_,
                                 const Settings &      settings_)
    : global_context(db_context.getGlobalContext()),
      path_pool(global_context.getPathPool().withTable(db_name_, table_name_, data_path_contains_database_name)),
      settings(settings_),
      storage_pool(db_name_ + "." + table_name_, path_pool, global_context, db_context.getSettingsRef()),
      db_name(db_name_),
      table_name(table_name_),
      is_common_handle(is_common_handle_),
      rowkey_column_size(rowkey_column_size_),
      original_table_handle_define(handle),
      background_pool(db_context.getBackgroundPool()),
      blockable_background_pool(db_context.getBlockableBackgroundPool()),
      next_gc_check_key(is_common_handle ? RowKeyValue::COMMON_HANDLE_MIN_KEY : RowKeyValue::INT_HANDLE_MIN_KEY),
      hash_salt(++DELTA_MERGE_STORE_HASH_SALT),
      log(&Logger::get("DeltaMergeStore[" + db_name + "." + table_name + "]"))
{
    LOG_INFO(log, "Restore DeltaMerge Store start [" << db_name << "." << table_name << "]");

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
    store_columns         = generateStoreColumns(original_table_columns, is_common_handle);

    auto dm_context = newDMContext(db_context, db_context.getSettingsRef());

    try
    {
        storage_pool.restore(); // restore from disk
        if (!storage_pool.maxMetaPageId())
        {
            // Create the first segment.
            auto segment_id = storage_pool.newMetaPageId();
            if (segment_id != DELTA_MERGE_FIRST_SEGMENT_ID)
                throw Exception("The first segment id should be " + DB::toString(DELTA_MERGE_FIRST_SEGMENT_ID), ErrorCodes::LOGICAL_ERROR);
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

    LOG_INFO(log, "Restore DeltaMerge Store end [" << db_name << "." << table_name << "]");
}

DeltaMergeStore::~DeltaMergeStore()
{
    LOG_INFO(log, "Release DeltaMerge Store start [" << db_name << "." << table_name << "]");

    shutdown();

    LOG_INFO(log, "Release DeltaMerge Store end [" << db_name << "." << table_name << "]");
}

void DeltaMergeStore::setUpBackgroundTask(const DMContextPtr & dm_context)
{
    auto dmfile_scanner = [=]() {
        PageStorage::PathAndIdsVec path_and_ids_vec;
        auto                       delegate = path_pool.getStableDiskDelegator();
        DMFile::ListOptions        options;
        options.only_list_can_gc = true;
        for (auto & root_path : delegate.listPaths())
        {
            auto & path_and_ids           = path_and_ids_vec.emplace_back();
            path_and_ids.first            = root_path;
            auto file_ids_in_current_path = DMFile::listAllInPath(global_context.getFileProvider(), root_path, options);
            for (auto id : file_ids_in_current_path)
                path_and_ids.second.insert(id);
        }
        return path_and_ids_vec;
    };
    auto dmfile_remover = [&](const PageStorage::PathAndIdsVec & path_and_ids_vec, const std::set<PageId> & valid_ids) {
        auto delegate = path_pool.getStableDiskDelegator();
        for (auto & [path, ids] : path_and_ids_vec)
        {
            for (auto id : ids)
            {
                if (valid_ids.count(id))
                    continue;

                // Note that ref_id is useless here.
                auto dmfile = DMFile::restore(global_context.getFileProvider(), id, /* ref_id= */ 0, path, false);
                if (dmfile->canGC())
                {
                    delegate.removeDTFile(dmfile->fileId());
                    dmfile->remove(global_context.getFileProvider());
                }

                LOG_DEBUG(log, "GC removed useless dmfile: " << dmfile->path());
            }
        }
    };
    storage_pool.data().registerExternalPagesCallbacks(dmfile_scanner, dmfile_remover);

    gc_handle              = background_pool.addTask([this] { return storage_pool.gc(global_context.getSettingsRef()); });
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
        LOG_WARNING(log,
                    "Applying heavy renaming for table " << db_name << "." << table_name //
                                                         << " to " << new_database_name << "." << new_table_name);

        // Remove all background task first
        shutdown();
        path_pool.rename(new_database_name, new_table_name, clean_rename); // rename for multi-disk
    }

    // TODO: replacing these two variables is not atomic, but could be good enough?
    table_name.swap(new_table_name);
    db_name.swap(new_database_name);
}

void DeltaMergeStore::drop()
{
    // Remove all background task first
    shutdown();

    LOG_INFO(log, "Drop DeltaMerge removing data from filesystem [" << db_name << "." << table_name << "]");
    storage_pool.drop();
    for (auto & [end, segment] : segments)
    {
        (void)end;
        segment->drop(global_context.getFileProvider());
    }
    // Drop data in storage path pool
    path_pool.drop(/*recursive=*/true, /*must_success=*/false);
    LOG_INFO(log, "Drop DeltaMerge done [" << db_name << "." << table_name << "]");

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

    LOG_TRACE(log, "Shutdown DeltaMerge start [" << db_name << "." << table_name << "]");
    background_pool.removeTask(gc_handle);
    gc_handle = nullptr;

    background_pool.removeTask(background_task_handle);
    blockable_background_pool.removeTask(blockable_background_pool_handle);
    background_task_handle           = nullptr;
    blockable_background_pool_handle = nullptr;
    LOG_TRACE(log, "Shutdown DeltaMerge end [" << db_name << "." << table_name << "]");
}

DMContextPtr DeltaMergeStore::newDMContext(const Context & db_context, const DB::Settings & db_settings, const String & query_id)
{
    std::shared_lock lock(read_write_mutex);

    // Here we use global context from db_context, instead of db_context directly.
    // Because db_context could be a temporary object and won't last long enough during the query process.
    // Like the context created by InterpreterSelectWithUnionQuery.
    auto * ctx = new DMContext(db_context.getGlobalContext(),
                               path_pool,
                               storage_pool,
                               hash_salt,
                               latest_gc_safe_point.load(std::memory_order_acquire),
                               settings.not_compress_columns,
                               is_common_handle,
                               rowkey_column_size,
                               db_settings,
                               query_id);
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

            auto sub_col      = c.cloneEmpty();
            sub_col.column    = std::move(column);
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
            FunctionToInt64::create(db_context)->execute(block, {handle_pos}, block.columns() - 1);
        }
        else
        {
            // If types are identical, `FunctionToInt64` just take reference to the original column.
            // We need a deep copy for the pk column or it will make trobule for later processing.
            auto      pk_col_with_name = getByColumnId(block, handle_define.id);
            auto      pk_column        = pk_col_with_name.column;
            ColumnPtr handle_column    = pk_column->cloneResized(pk_column->size());
            addColumnToBlock(block, //
                             EXTRA_HANDLE_COLUMN_ID,
                             EXTRA_HANDLE_COLUMN_NAME,
                             EXTRA_HANDLE_COLUMN_INT_TYPE,
                             handle_column);
        }
    }
    return std::move(block);
}

void DeltaMergeStore::write(const Context & db_context, const DB::Settings & db_settings, Block && to_write)
{
    LOG_TRACE(log, __FUNCTION__ << " table: " << db_name << "." << table_name << ", rows: " << to_write.rows());

    EventRecorder write_block_recorder(ProfileEvents::DMWriteBlock, ProfileEvents::DMWriteBlockNS);

    const auto rows = to_write.rows();
    if (rows == 0)
        return;

    auto  dm_context = newDMContext(db_context, db_settings);
    Block block      = addExtraColumnIfNeed(db_context, original_table_handle_define, std::move(to_write));

    const auto bytes = block.bytes();

    {
        // Sort by handle & version in ascending order.
        SortDescription sort;
        sort.emplace_back(EXTRA_HANDLE_COLUMN_NAME, 1, 0);
        sort.emplace_back(VERSION_COLUMN_NAME, 1, 0);

        if (rows > 1 && !isAlreadySorted(block, sort))
            stableSortBlock(block, sort);
    }

    Segments updated_segments;

    size_t     offset = 0;
    size_t     limit;
    const auto handle_column = block.getByName(EXTRA_HANDLE_COLUMN_NAME).column;
    auto       rowkey_column = RowKeyColumnContainer(handle_column, is_common_handle);

    while (offset != rows)
    {
        RowKeyValueRef start_key = rowkey_column.getRowKeyValue(offset);
        WriteBatches   wbs(storage_pool);
        DeltaPackPtr   write_pack;
        RowKeyRange    write_range;

        // Keep trying until succeeded.
        while (true)
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(start_key);
                if (segment_it == segments.end())
                {
                    // todo print meaningful start row key
                    throw Exception("Failed to locate segment begin with start: " + start_key.toDebugString(), ErrorCodes::LOGICAL_ERROR);
                }
                segment = segment_it->second;
            }

            FAIL_POINT_PAUSE(FailPoints::pause_when_writing_to_dt_store);
            waitForWrite(dm_context, segment);
            if (segment->hasAbandoned())
                continue;

            auto & rowkey_range = segment->getRowKeyRange();

            auto [cur_offset, cur_limit] = rowkey_range.getPosRange(handle_column, offset, rows - offset);
            if (unlikely(cur_offset != offset))
                throw Exception("cur_offset does not equal to offset", ErrorCodes::LOGICAL_ERROR);

            limit      = cur_limit;
            auto bytes = block.bytes(offset, limit);

            bool small_pack = limit < dm_context->delta_cache_limit_rows / 4 && bytes < dm_context->delta_cache_limit_bytes / 4;
            // Small packs are appended to Delta Cache, the flushed later.
            // While large packs are directly written to PageStorage.
            if (small_pack)
            {
                if (segment->writeToCache(*dm_context, block, offset, limit))
                {
                    updated_segments.push_back(segment);
                    break;
                }
            }
            else
            {
                // If pack haven't been written, or the pk range has changed since last write, then write it and
                // delete former written pack.
                if (!write_pack || (write_pack && write_range != rowkey_range))
                {
                    wbs.rollbackWrittenLogAndData();
                    wbs.clear();

                    write_pack = DeltaPackBlock::writePack(*dm_context, block, offset, limit, wbs);
                    wbs.writeLogAndData();
                    write_range = rowkey_range;
                }

                // Write could fail, because other threads could already updated the instance. Like split/merge, merge delta.
                if (segment->writeToDisk(*dm_context, write_pack))
                {
                    updated_segments.push_back(segment);
                    break;
                }
            }
        }

        offset += limit;
    }

    GET_METRIC(dm_context->metrics, tiflash_storage_throughput_bytes, type_write).Increment(bytes);
    GET_METRIC(dm_context->metrics, tiflash_storage_throughput_rows, type_write).Increment(rows);

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

    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);
}

std::tuple<String, PageId> DeltaMergeStore::preAllocateIngestFile()
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return {};

    auto delegator   = path_pool.getStableDiskDelegator();
    auto parent_path = delegator.choosePath();
    auto new_id      = storage_pool.newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    return {parent_path, new_id};
}

void DeltaMergeStore::preIngestFile(const String & parent_path, const PageId file_id, size_t file_size)
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return;

    auto delegator = path_pool.getStableDiskDelegator();
    delegator.addDTFile(file_id, file_size, parent_path);
}

void DeltaMergeStore::ingestFiles(const DMContextPtr &        dm_context,
                                  const RowKeyRange &         range,
                                  const std::vector<PageId> & file_ids,
                                  bool                        clear_data_in_range)
{
    if (unlikely(shutdown_called.load(std::memory_order_relaxed)))
    {
        std::stringstream stream;
        stream << " try to ingest files into a shutdown table: " << db_name << "." << table_name;
        auto msg = stream.str();
        LOG_WARNING(log, __FUNCTION__ << msg);
        throw Exception(msg);
    }

    EventRecorder write_block_recorder(ProfileEvents::DMWriteFile, ProfileEvents::DMWriteFileNS);

    auto delegate      = dm_context->path_pool.getStableDiskDelegator();
    auto file_provider = dm_context->db_context.getFileProvider();

    size_t rows          = 0;
    size_t bytes         = 0;
    size_t bytes_on_disk = 0;

    DMFiles files;
    for (auto file_id : file_ids)
    {
        auto file_parent_path = delegate.getDTFilePath(file_id);

        auto file = DMFile::restore(file_provider, file_id, file_id, file_parent_path);
        rows += file->getRows();
        bytes += file->getBytes();
        bytes_on_disk += file->getBytesOnDisk();

        files.emplace_back(std::move(file));
    }

    LOG_INFO(log,
             __FUNCTION__ << " table: " << db_name << "." << table_name << ", rows: " << rows << ", bytes: " << bytes << ", bytes on disk: "
                          << bytes_on_disk << ", region range: " << range.toDebugString() << ", clear_data: " << clear_data_in_range);

    Segments    updated_segments;
    RowKeyRange cur_range = range;

    // Put the ingest file ids into `storage_pool` and use ref id in each segments to ensure the atomic
    // of ingesting.
    // Check https://github.com/pingcap/tics/issues/2040 for more details.
    // TODO: If tiflash crash during the middle of ingesting, we may leave some DTFiles on disk and
    // they can not be deleted. We should find a way to cleanup those files.
    WriteBatches ingest_wbs(storage_pool);
    if (files.size() > 0)
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
                    throw Exception("Failed to locate segment begin with start in range: " + cur_range.toDebugString(),
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
            DeltaPacks   packs;
            WriteBatches wbs(storage_pool);

            for (const auto & file : files)
            {
                /// Generate DMFile instance with a new ref_id pointed to the file_id.
                auto   file_id          = file->fileId();
                auto & file_parent_path = file->parentPath();
                auto   ref_id           = storage_pool.newDataPageIdForDTFile(delegate, __PRETTY_FUNCTION__);

                auto ref_file = DMFile::restore(file_provider, file_id, ref_id, file_parent_path);
                auto pack     = std::make_shared<DeltaPackFile>(*dm_context, ref_file, segment_range);
                if (pack->getRows() != 0)
                {
                    packs.emplace_back(std::move(pack));
                    wbs.data.putRefPage(ref_id, file_id);
                }
            }

            // We have to commit those file_ids to PageStorage, because as soon as packs are written into segments,
            // they are visible for readers who require file_ids to be found in PageStorage.
            wbs.writeLogAndData();

            bool ingest_success = segment->ingestPacks(*dm_context, range.shrink(segment_range), packs, clear_data_in_range);
            fiu_do_on(FailPoints::force_set_segment_ingest_packs_fail, { ingest_success = false; });
            if (ingest_success)
            {
                updated_segments.push_back(segment);
                fiu_do_on(FailPoints::segment_merge_after_ingest_packs, {
                    segment->flushCache(*dm_context);
                    segmentMergeDelta(*dm_context, segment, TaskRunThread::BackgroundThreadPool);
                    storage_pool.gc(global_context.getSettingsRef(), StoragePool::Seconds(0));
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
        std::stringstream ss;
        // Example: "ingest dmf_1001,1002,1003 into segment [1,3]"
        //          "ingest <empty> into segment [1,3]"
        if (file_ids.empty())
        {
            ss << "ingest <empty>";
        }
        else
        {
            ss << "ingest dmf_";
            for (size_t i = 0; i < file_ids.size(); ++i)
            {
                if (i != 0)
                    ss << ",";
                ss << file_ids[i];
            }
        }
        ss << " into segment [";
        for (size_t i = 0; i < updated_segments.size(); ++i)
        {
            if (i != 0)
                ss << ",";
            ss << updated_segments[i]->segmentId();
        }
        ss << "]";
        LOG_INFO(log,
                 __FUNCTION__ << " table: " << db_name << "." << table_name << ", clear_data: " << clear_data_in_range << ", " << ss.str());
    }

    GET_METRIC(dm_context->metrics, tiflash_storage_throughput_bytes, type_ingest).Increment(bytes);
    GET_METRIC(dm_context->metrics, tiflash_storage_throughput_rows, type_ingest).Increment(rows);

    flushCache(dm_context, range);

    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);
}

void DeltaMergeStore::deleteRange(const Context & db_context, const DB::Settings & db_settings, const RowKeyRange & delete_range)
{
    LOG_INFO(log, __FUNCTION__ << " table: " << db_name << "." << table_name << " delete range " << delete_range.toDebugString());

    EventRecorder write_block_recorder(ProfileEvents::DMDeleteRange, ProfileEvents::DMDeleteRangeNS);

    if (delete_range.none())
        return;

    auto dm_context = newDMContext(db_context, db_settings);

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
                    throw Exception("Failed to locate segment begin with start in range: " + cur_range.toDebugString(),
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

    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);
}

void DeltaMergeStore::flushCache(const DMContextPtr & dm_context, const RowKeyRange & range)
{
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
                    throw Exception("Failed to locate segment begin with start in range: " + cur_range.toDebugString(),
                                    ErrorCodes::LOGICAL_ERROR);
                }
                segment = segment_it->second;
            }
            segment_range = segment->getRowKeyRange();

            // Flush could fail.
            if (segment->flushCache(*dm_context))
            {
                break;
            }
        }

        cur_range.setStart(segment_range.end);
    }
}

void DeltaMergeStore::mergeDeltaAll(const Context & context)
{
    auto dm_context = newDMContext(context, context.getSettingsRef());

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

void DeltaMergeStore::compact(const Context & db_context, const RowKeyRange & range)
{
    auto dm_context = newDMContext(db_context, db_context.getSettingsRef());

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
                    throw Exception("Failed to locate segment begin with start in range: " + cur_range.toDebugString(),
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

BlockInputStreams DeltaMergeStore::readRaw(const Context &       db_context,
                                           const DB::Settings &  db_settings,
                                           const ColumnDefines & columns_to_read,
                                           size_t                num_streams,
                                           const SegmentIdSet &  read_segments)
{
    SegmentReadTasks tasks;

    auto dm_context = newDMContext(db_context, db_settings, db_context.getCurrentQueryId());

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

    auto after_segment_read = [&](const DMContextPtr & dm_context_, const SegmentPtr & segment_) {
        this->checkSegmentUpdate(dm_context_, segment_, ThreadType::Read);
    };
    size_t final_num_stream = std::min(num_streams, tasks.size());
    auto   read_task_pool   = std::make_shared<SegmentReadTaskPool>(std::move(tasks));

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>( //
            dm_context,
            read_task_pool,
            after_segment_read,
            columns_to_read,
            EMPTY_FILTER,
            MAX_UINT64,
            DEFAULT_BLOCK_SIZE,
            true,
            db_settings.dt_raw_filter_range);
        res.push_back(stream);
    }
    return res;
}

BlockInputStreams DeltaMergeStore::read(const Context &       db_context,
                                        const DB::Settings &  db_settings,
                                        const ColumnDefines & columns_to_read,
                                        const RowKeyRanges &  sorted_ranges,
                                        size_t                num_streams,
                                        UInt64                max_version,
                                        const RSOperatorPtr & filter,
                                        size_t                expected_block_size,
                                        const SegmentIdSet &  read_segments)
{
    auto dm_context = newDMContext(db_context, db_settings, db_context.getCurrentQueryId());

    SegmentReadTasks tasks = getReadTasksByRanges(*dm_context, sorted_ranges, num_streams, read_segments);

    LOG_DEBUG(log, "Read create segment snapshot done");

    auto after_segment_read = [&](const DMContextPtr & dm_context_, const SegmentPtr & segment_) {
        this->checkSegmentUpdate(dm_context_, segment_, ThreadType::Read);
    };

    GET_METRIC(dm_context->metrics, tiflash_storage_read_tasks_count).Increment(tasks.size());
    size_t final_num_stream = std::max(1, std::min(num_streams, tasks.size()));
    auto   read_task_pool   = std::make_shared<SegmentReadTaskPool>(std::move(tasks));

    BlockInputStreams res;
    for (size_t i = 0; i < final_num_stream; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DMSegmentThreadInputStream>( //
            dm_context,
            read_task_pool,
            after_segment_read,
            columns_to_read,
            filter,
            max_version,
            expected_block_size,
            false,
            db_settings.dt_raw_filter_range);
        res.push_back(stream);
    }

    LOG_DEBUG(log, "Read create stream done");

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
    size_t delta_rows  = segment->getDelta()->getRows();
    size_t delta_bytes = segment->getDelta()->getBytes();

    if (delta_rows < forceMergeDeltaRows(dm_context) && delta_bytes < forceMergeDeltaBytes(dm_context))
        return;

    Stopwatch watch;
    SCOPE_EXIT(
        { GET_METRIC(dm_context->metrics, tiflash_storage_write_stall_duration_seconds, type_write).Observe(watch.elapsedSeconds()); });

    size_t segment_bytes = segment->getEstimatedBytes();
    // The speed of delta merge in a very bad situation we assume. It should be a very conservative value.
    size_t _10MB = 10 << 20;

    size_t stop_write_delta_rows  = dm_context->db_context.getSettingsRef().dt_segment_stop_write_delta_rows;
    size_t stop_write_delta_bytes = dm_context->db_context.getSettingsRef().dt_segment_stop_write_delta_size;
    size_t wait_duration_factor   = dm_context->db_context.getSettingsRef().dt_segment_wait_duration_factor;

    size_t sleep_ms;
    if (delta_rows >= stop_write_delta_rows || delta_bytes >= stop_write_delta_bytes)
        sleep_ms = std::numeric_limits<size_t>::max();
    else
        sleep_ms = (double)segment_bytes / _10MB * 1000 * wait_duration_factor;

    // checkSegmentUpdate could do foreground merge delta, so call it before sleep.
    checkSegmentUpdate(dm_context, segment, ThreadType::Write);

    size_t sleep_step = 50;
    // The delta will be merged, only after this segment got abandoned.
    // Because merge delta will replace the segment instance.
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
    if (segment->hasAbandoned())
        return;
    auto & delta = segment->getDelta();

    size_t delta_saved_rows  = delta->getRows(/* use_unsaved */ false);
    size_t delta_saved_bytes = delta->getBytes(/* use_unsaved */ false);
    size_t delta_check_rows  = std::max(delta->updatesInDeltaTree(), delta_saved_rows);
    size_t delta_check_bytes = delta_saved_bytes;

    size_t delta_deletes = delta->getDeletes();

    size_t unsaved_rows  = delta->getUnsavedRows();
    size_t unsaved_bytes = delta->getUnsavedBytes();

    size_t delta_rows    = delta_saved_rows + unsaved_rows;
    size_t delta_bytes   = delta_saved_bytes + unsaved_bytes;
    size_t segment_rows  = segment->getEstimatedRows();
    size_t segment_bytes = segment->getEstimatedBytes();
    size_t pack_count    = delta->getPackCount();

    size_t placed_delta_rows = delta->getPlacedDeltaRows();

    auto & delta_last_try_flush_rows             = delta->getLastTryFlushRows();
    auto & delta_last_try_flush_bytes            = delta->getLastTryFlushBytes();
    auto & delta_last_try_compact_packs          = delta->getLastTryCompactPacks();
    auto & delta_last_try_merge_delta_rows       = delta->getLastTryMergeDeltaRows();
    auto & delta_last_try_merge_delta_bytes      = delta->getLastTryMergeDeltaBytes();
    auto & delta_last_try_split_rows             = delta->getLastTrySplitRows();
    auto & delta_last_try_split_bytes            = delta->getLastTrySplitBytes();
    auto & delta_last_try_place_delta_index_rows = delta->getLastTryPlaceDeltaIndexRows();

    auto segment_limit_rows      = dm_context->segment_limit_rows;
    auto segment_limit_bytes     = dm_context->segment_limit_bytes;
    auto delta_limit_rows        = dm_context->delta_limit_rows;
    auto delta_limit_bytes       = dm_context->delta_limit_bytes;
    auto delta_cache_limit_rows  = dm_context->delta_cache_limit_rows;
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
    bool should_split = (segment_rows >= segment_limit_rows * 2 || segment_bytes >= segment_limit_bytes * 2)
        && (delta_rows - delta_last_try_split_rows >= delta_cache_limit_rows
            || delta_bytes - delta_last_try_split_bytes >= delta_cache_limit_bytes);

    bool should_merge = segment_rows < segment_limit_rows / 3 && segment_bytes < segment_limit_bytes / 3;

    // Don't do compact on starting up.
    bool should_compact = (thread_type != ThreadType::Init) && std::max((Int64)pack_count - delta_last_try_compact_packs, 0) >= 10;

    // Don't do background place index if we limit DeltaIndex cache.
    bool should_place_delta_index = !dm_context->db_context.isDeltaIndexLimited()
        && (delta_rows - placed_delta_rows >= delta_cache_limit_rows * 3
            && delta_rows - delta_last_try_place_delta_index_rows >= delta_cache_limit_rows);

    fiu_do_on(FailPoints::force_triggle_background_merge_delta, { should_background_merge_delta = true; });
    fiu_do_on(FailPoints::force_triggle_foreground_flush, { should_foreground_flush = true; });

    auto try_add_background_task = [&](const BackgroundTask & task) {
        // Prevent too many tasks.
        if (background_tasks.length() <= std::max(id_to_segment.size() * 2, background_pool.getNumberOfThreads() * 3))
        {
            if (shutdown_called.load(std::memory_order_relaxed))
                return;

            auto heavy = background_tasks.addTask(task, thread_type, log);
            if (heavy)
                blockable_background_pool_handle->wake();
            else
                background_task_handle->wake();
        }
    };

    /// Flush is always try first.
    if (thread_type != ThreadType::Read)
    {
        if (should_foreground_flush)
        {
            delta_last_try_flush_rows  = delta_rows;
            delta_last_try_flush_bytes = delta_bytes;
            LOG_DEBUG(log, "Foreground flush cache " << segment->info());
            segment->flushCache(*dm_context);
        }
        else if (should_background_flush)
        {
            delta_last_try_flush_rows  = delta_rows;
            delta_last_try_flush_bytes = delta_bytes;
            try_add_background_task(BackgroundTask{TaskType::Flush, dm_context, segment, {}});
        }
    }

    // Need to check the latest delta (maybe updated after foreground flush). If it is updating by another thread,
    // give up adding more tasks on this version of delta.
    if (segment->getDelta()->isUpdating())
        return;

    /// Now start trying structure update.

    auto getMergeSibling = [&]() -> SegmentPtr {
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
            auto limit   = dm_context->segment_limit_rows / 5;
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
                    GET_METRIC(dm_context->metrics, tiflash_storage_write_stall_duration_seconds, type_write)
                        .Observe(watch.elapsedSeconds());
                if (should_foreground_merge_delta_by_deletes)
                    GET_METRIC(dm_context->metrics, tiflash_storage_write_stall_duration_seconds, type_delete_range)
                        .Observe(watch.elapsedSeconds());
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
        if (should_split && !seg->isSplitForbidden())
        {
            delta_last_try_split_rows  = delta_rows;
            delta_last_try_split_bytes = delta_bytes;
            try_add_background_task(BackgroundTask{TaskType::Split, dm_context, seg, {}});
            return true;
        }
        return false;
    };
    auto try_fg_split = [&](const SegmentPtr & my_segment) -> bool {
        auto my_segment_rows = my_segment->getEstimatedRows();
        auto my_should_split = my_segment_rows >= dm_context->segment_limit_rows * 3;
        if (my_should_split && !my_segment->isSplitForbidden())
        {
            if (segmentSplit(*dm_context, my_segment, true).first)
                return true;
            else
                return false;
        }
        return false;
    };
    auto try_bg_merge = [&]() {
        SegmentPtr merge_sibling;
        if (should_merge && (merge_sibling = getMergeSibling()))
        {
            try_add_background_task(BackgroundTask{TaskType::Merge, dm_context, segment, merge_sibling});
            return true;
        }
        return false;
    };
    auto try_bg_compact = [&]() {
        if (should_compact)
        {
            delta_last_try_compact_packs = pack_count;
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
        auto safe_point = PDClientHelper::getGCSafePointWithRetry(pd_client,
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
        LOG_DEBUG(log, "Task " << toString(task.type) << " GC safe point: " << task.dm_context->min_version);
    }

    SegmentPtr left, right;
    ThreadType type = ThreadType::Write;
    try
    {
        switch (task.type)
        {
        case TaskType::Split:
            std::tie(left, right) = segmentSplit(*task.dm_context, task.segment, false);
            type                  = ThreadType::BG_Split;
            break;
        case TaskType::Merge:
            segmentMerge(*task.dm_context, task.segment, task.next_segment, false);
            type = ThreadType::BG_Merge;
            break;
        case TaskType::MergeDelta: {
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
            throw Exception("Unsupported task type: " + DeltaMergeStore::toString(task.type));
        }
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log,
                  "Task " << DeltaMergeStore::toString(task.type) << " on Segment [" << task.segment->segmentId()
                          << ((bool)task.next_segment ? ("] and [" + DB::toString(task.next_segment->segmentId())) : "")
                          << "] failed. Error msg: " << e.message());
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
bool shouldCompactStable(const SegmentPtr & seg, DB::Timestamp gc_safepoint, double ratio_threshold, Logger * log)
{
    // Always GC.
    if (ratio_threshold < 1.0)
        return true;

    auto & property = seg->getStable()->getStableProperty();
    LOG_TRACE(log, __PRETTY_FUNCTION__ << property.toDebugString());
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

bool shouldCompactDeltaWithStable(
    const DMContext & context, const SegmentSnapshotPtr & snap, const RowKeyRange & segment_range, double ratio_threshold, Logger * log)
{
    auto actual_delete_range = snap->delta->getSquashDeleteRange().shrink(segment_range);
    if (actual_delete_range.none())
        return false;

    auto [delete_rows, delete_bytes] = snap->stable->getApproxRowsAndBytes(context, actual_delete_range);

    auto stable_rows  = snap->stable->getRows();
    auto stable_bytes = snap->stable->getBytes();

    LOG_TRACE(log,
              __PRETTY_FUNCTION__ << " delete range rows [" << delete_rows << "], delete_bytes [" << delete_bytes << "] stable_rows ["
                                  << stable_rows << "] stable_bytes [" << stable_bytes << "]");

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
    LOG_DEBUG(log,
              "GC on table " << table_name << " start with key: " << next_gc_check_key.toDebugString()
                             << ", gc_safe_point: " << gc_safe_point << ", max gc limit: " << limit);

    UInt64 check_segments_num = 0;
    Int64  gc_segments_num    = 0;
    while (gc_segments_num < limit)
    {
        // If the store is shut down, give up running GC on it.
        if (shutdown_called.load(std::memory_order_relaxed))
            break;

        auto               dm_context = newDMContext(global_context, global_context.getSettingsRef());
        SegmentPtr         segment;
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

            segment           = segment_it->second;
            next_gc_check_key = segment_it->first.toRowKeyValue();
            segment_snap      = segment->createSnapshot(*dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfDeltaMerge);
        }

        assert(segment != nullptr);
        if (segment->hasAbandoned() || segment->getLastCheckGCSafePoint() >= gc_safe_point || segment_snap == nullptr)
            continue;

        const auto  segment_id    = segment->segmentId();
        RowKeyRange segment_range = segment->getRowKeyRange();

        // meet empty segment, try merge it
        if (segment_snap->getRows() == 0)
        {
            checkSegmentUpdate(dm_context, segment, ThreadType::BG_GC);
            continue;
        }

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

        try
        {
            // Check whether we should apply gc on this segment
            const bool should_compact
                = GC::shouldCompactStable(
                      segment, gc_safe_point, global_context.getSettingsRef().dt_bg_gc_ratio_threhold_to_trigger_gc, log)
                || GC::shouldCompactDeltaWithStable(*dm_context,
                                                    segment_snap,
                                                    segment_range,
                                                    global_context.getSettingsRef().dt_bg_gc_delta_delete_ratio_to_trigger_gc,
                                                    log);
            bool finish_gc_on_segment = false;
            if (should_compact)
            {
                if (segment = segmentMergeDelta(*dm_context, segment, TaskRunThread::BackgroundGCThread, segment_snap); segment)
                {
                    // Continue to check whether we need to apply more tasks on this segment
                    checkSegmentUpdate(dm_context, segment, ThreadType::BG_GC);
                    gc_segments_num++;
                    finish_gc_on_segment = true;
                    LOG_INFO(log,
                             "GC-merge-delta done Segment [" << segment_id << "] [range=" << segment_range.toDebugString()
                                                             << "] [table=" << table_name << "]");
                }
                else
                {
                    LOG_INFO(log,
                             "GC aborted on Segment [" << segment_id << "] [range=" << segment_range.toDebugString()
                                                       << "] [table=" << table_name << "]");
                }
            }
            if (!finish_gc_on_segment)
                LOG_DEBUG(log,
                          "GC is skipped Segment [" << segment_id << "] [range=" << segment_range.toDebugString()
                                                    << "] [table=" << table_name << "]");
        }
        catch (Exception & e)
        {
            e.addMessage("while apply gc Segment [" + DB::toString(segment_id) + "] [range=" + segment_range.toDebugString()
                         + "] [table=" + table_name + "]");
            e.rethrow();
        }
    }

    LOG_DEBUG(log, "Finish GC on " << gc_segments_num << " segments [table=" + table_name + "]");
    return gc_segments_num;
}

SegmentPair DeltaMergeStore::segmentSplit(DMContext & dm_context, const SegmentPtr & segment, bool is_foreground)
{
    LOG_DEBUG(log,
              (is_foreground ? "Foreground" : "Background")
                  << " split segment " << segment->info() << ", safe point:" << dm_context.min_version);

    SegmentSnapshotPtr segment_snap;
    ColumnDefinesPtr   schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
        {
            LOG_DEBUG(log, "Give up segment [" << segment->segmentId() << "] split");
            return {};
        }

        segment_snap = segment->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentSplit);
        if (!segment_snap || !segment_snap->getRows())
        {
            LOG_DEBUG(log, "Give up segment [" << segment->segmentId() << "] split");
            return {};
        }
        schema_snap = store_columns;
    }

    // Not counting the early give up action.
    auto delta_bytes = (Int64)segment_snap->delta->getBytes();
    auto delta_rows  = (Int64)segment_snap->delta->getRows();

    size_t duplicated_bytes = 0;
    size_t duplicated_rows  = 0;

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_SegmentSplit};
    GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_seg_split).Increment();
    Stopwatch watch_seg_split;
    SCOPE_EXIT({
        GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_seg_split).Observe(watch_seg_split.elapsedSeconds());
    });

    WriteBatches wbs(storage_pool, is_foreground ? nullptr : dm_context.db_context.getRateLimiter());

    auto range          = segment->getRowKeyRange();
    auto split_info_opt = segment->prepareSplit(dm_context, schema_snap, segment_snap, wbs, !is_foreground);

    if (!split_info_opt.has_value())
    {
        // Likely we can not find an appropriate split point for this segment later, forbid the split until this segment get updated through applying delta-merge. Or it will slow down the write a lot.
        segment->forbidSplit();
        LOG_WARNING(log, "Giving up and forbid later split. Segment [" << segment->segmentId() << "]. Because of prepare split failed");
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
            LOG_DEBUG(log, "Give up segment [" << segment->segmentId() << "] split");
            wbs.setRollback();
            return {};
        }

        LOG_DEBUG(log, "Apply split. Segment [" << segment->segmentId() << "]");

        auto segment_lock = segment->mustGetUpdateLock();

        std::tie(new_left, new_right) = segment->applySplit(dm_context, segment_snap, wbs, split_info);

        wbs.writeMeta();

        segment->abandon(dm_context);
        segments.erase(range.getEnd());
        id_to_segment.erase(segment->segmentId());

        segments[new_left->getRowKeyRange().getEnd()]  = new_left;
        segments[new_right->getRowKeyRange().getEnd()] = new_right;

        id_to_segment.emplace(new_left->segmentId(), new_left);
        id_to_segment.emplace(new_right->segmentId(), new_right);

        if constexpr (DM_RUN_CHECK)
        {
            new_left->check(dm_context, "After split left");
            new_right->check(dm_context, "After split right");
        }

        duplicated_bytes = new_left->getDelta()->getBytes();
        duplicated_rows  = new_right->getDelta()->getBytes();

        LOG_DEBUG(log, "Apply split done. Segment [" << segment->segmentId() << "]");
    }

    wbs.writeRemoves();

    if (!split_info.is_logical)
    {
        GET_METRIC(dm_context.metrics, tiflash_storage_throughput_bytes, type_split).Increment(delta_bytes);
        GET_METRIC(dm_context.metrics, tiflash_storage_throughput_rows, type_split).Increment(delta_rows);
    }
    else
    {
        // For logical split, delta is duplicated into two segments. And will be merged into stable twice later. So we need to decrease it here.
        // Otherwise the final total delta merge bytes is greater than bytes written into.
        GET_METRIC(dm_context.metrics, tiflash_storage_throughput_bytes, type_split).Decrement(duplicated_bytes);
        GET_METRIC(dm_context.metrics, tiflash_storage_throughput_rows, type_split).Decrement(duplicated_rows);
    }

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);

    return {new_left, new_right};
}

void DeltaMergeStore::segmentMerge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right, bool is_foreground)
{
    LOG_DEBUG(log,
              (is_foreground ? "Foreground" : "Background")
                  << " merge Segment [" << left->info() << "] and [" << right->info() << "], safe point:" << dm_context.min_version);

    /// This segment may contain some rows that not belong to this segment range which is left by previous split operation.
    /// And only saved data in this segment will be filtered by the segment range in the merge process,
    /// unsaved data will be directly copied to the new segment.
    /// So we flush here to make sure that all potential data left by previous split operation is saved.
    while (!left->flushCache(dm_context))
    {
        // keep flush until success if not abandoned
        if (left->hasAbandoned())
        {
            LOG_DEBUG(log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");
            return;
        }
    }
    while (!right->flushCache(dm_context))
    {
        // keep flush until success if not abandoned
        if (right->hasAbandoned())
        {
            LOG_DEBUG(log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");
            return;
        }
    }

    SegmentSnapshotPtr left_snap;
    SegmentSnapshotPtr right_snap;
    ColumnDefinesPtr   schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, left))
        {
            LOG_DEBUG(log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");
            return;
        }
        if (!isSegmentValid(lock, right))
        {
            LOG_DEBUG(log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");
            return;
        }

        left_snap  = left->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
        right_snap = right->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentMerge);

        if (!left_snap || !right_snap)
        {
            LOG_DEBUG(log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");
            return;
        }
        schema_snap = store_columns;
    }

    // Not counting the early give up action.
    auto delta_bytes = (Int64)left_snap->delta->getBytes() + right_snap->getBytes();
    auto delta_rows  = (Int64)left_snap->delta->getRows() + right_snap->getRows();

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_SegmentMerge};
    GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_seg_merge).Increment();
    Stopwatch watch_seg_merge;
    SCOPE_EXIT({
        GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_seg_merge).Observe(watch_seg_merge.elapsedSeconds());
    });

    auto left_range  = left->getRowKeyRange();
    auto right_range = right->getRowKeyRange();

    WriteBatches wbs(storage_pool, is_foreground ? nullptr : dm_context.db_context.getRateLimiter());
    auto         merged_stable = Segment::prepareMerge(dm_context, schema_snap, left, left_snap, right, right_snap, wbs, !is_foreground);
    wbs.writeLogAndData();
    merged_stable->enableDMFilesGC();

    {
        std::unique_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, left) || !isSegmentValid(lock, right))
        {
            LOG_DEBUG(log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");
            wbs.setRollback();
            return;
        }

        LOG_DEBUG(log, "Apply merge. Left [" << left->segmentId() << "], right [" << right->segmentId() << "]");

        auto left_lock  = left->mustGetUpdateLock();
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

        LOG_DEBUG(log, "Apply merge done. [" << left->info() << "] and [" << right->info() << "]");
    }

    wbs.writeRemoves();

    GET_METRIC(dm_context.metrics, tiflash_storage_throughput_bytes, type_merge).Increment(delta_bytes);
    GET_METRIC(dm_context.metrics, tiflash_storage_throughput_rows, type_merge).Increment(delta_rows);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);
}

SegmentPtr DeltaMergeStore::segmentMergeDelta(DMContext &         dm_context,
                                              const SegmentPtr &  segment,
                                              const TaskRunThread run_thread,
                                              SegmentSnapshotPtr  segment_snap)
{
    LOG_DEBUG(log, toString(run_thread) << " merge delta, segment [" << segment->segmentId() << "], safe point:" << dm_context.min_version);

    ColumnDefinesPtr schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
        {
            LOG_DEBUG(log, "Give up merge delta, segment [" << segment->segmentId() << "]");
            return {};
        }

        // Try to generate a new snapshot if there is no pre-allocated one
        if (!segment_snap)
            segment_snap = segment->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfDeltaMerge);

        if (unlikely(!segment_snap))
        {
            LOG_DEBUG(log, "Give up merge delta, segment [" << segment->segmentId() << "]");
            return {};
        }
        schema_snap = store_columns;
    }

    // Not counting the early give up action.
    auto delta_bytes = (Int64)segment_snap->delta->getBytes();
    auto delta_rows  = (Int64)segment_snap->delta->getRows();

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_DeltaMerge};
    CurrentMetrics::Increment cur_dm_total_bytes{CurrentMetrics::DT_DeltaMergeTotalBytes, (Int64)segment_snap->getBytes()};
    CurrentMetrics::Increment cur_dm_total_rows{CurrentMetrics::DT_DeltaMergeTotalRows, (Int64)segment_snap->getRows()};

    switch (run_thread)
    {
    case TaskRunThread::BackgroundThreadPool:
        GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_delta_merge).Increment();
        break;
    case TaskRunThread::Foreground:
        GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_delta_merge_fg).Increment();
        break;
    case TaskRunThread::BackgroundGCThread:
        GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_delta_merge_bg_gc).Increment();
        break;
    default:
        break;
    }

    Stopwatch watch_delta_merge;
    SCOPE_EXIT({
        switch (run_thread)
        {
        case TaskRunThread::BackgroundThreadPool:
            GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_delta_merge)
                .Observe(watch_delta_merge.elapsedSeconds());
            break;
        case TaskRunThread::Foreground:
            GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_delta_merge_fg)
                .Observe(watch_delta_merge.elapsedSeconds());
            break;
        case TaskRunThread::BackgroundGCThread:
            GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_delta_merge_bg_gc)
                .Observe(watch_delta_merge.elapsedSeconds());
            break;
        default:
            break;
        }
    });

    bool         need_rate_limit = (run_thread != TaskRunThread::Foreground);
    WriteBatches wbs(storage_pool, need_rate_limit ? dm_context.db_context.getRateLimiter() : nullptr);

    auto new_stable = segment->prepareMergeDelta(dm_context, schema_snap, segment_snap, wbs, need_rate_limit);
    wbs.writeLogAndData();
    new_stable->enableDMFilesGC();

    SegmentPtr new_segment;
    {
        std::unique_lock read_write_lock(read_write_mutex);

        if (!isSegmentValid(read_write_lock, segment))
        {
            LOG_DEBUG(log, "Give up merge delta, segment [" << segment->segmentId() << "]");
            wbs.setRollback();
            return {};
        }

        LOG_DEBUG(log, "Apply merge delta. Segment [" << segment->info() << "]");

        auto segment_lock = segment->mustGetUpdateLock();

        new_segment = segment->applyMergeDelta(dm_context, segment_snap, wbs, new_stable);

        wbs.writeMeta();


        // The instance of PKRange::End is closely linked to instance of PKRange. So we cannot reuse it.
        // Replace must be done by erase + insert.
        segments.erase(segment->getRowKeyRange().getEnd());
        id_to_segment.erase(segment->segmentId());

        segments[new_segment->getRowKeyRange().getEnd()] = new_segment;
        id_to_segment[new_segment->segmentId()]          = new_segment;

        segment->abandon(dm_context);

        if constexpr (DM_RUN_CHECK)
        {
            new_segment->check(dm_context, "After merge delta");
        }

        LOG_DEBUG(log, "Apply merge delta done. Segment [" << segment->segmentId() << "]");
    }

    wbs.writeRemoves();

    GET_METRIC(dm_context.metrics, tiflash_storage_throughput_bytes, type_delta_merge).Increment(delta_bytes);
    GET_METRIC(dm_context.metrics, tiflash_storage_throughput_rows, type_delta_merge).Increment(delta_rows);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);

    return new_segment;
}

bool DeltaMergeStore::doIsSegmentValid(const SegmentPtr & segment)
{
    if (segment->hasAbandoned())
    {
        LOG_DEBUG(log, "Segment [" << segment->segmentId() << "] instance has abandoned");
        return false;
    }
    // Segment instance could have been removed or replaced.
    auto it = segments.find(segment->getRowKeyRange().getEnd());
    if (it == segments.end())
    {
        LOG_DEBUG(log, "Segment [" << segment->segmentId() << "] not found in segment map");

        auto it2 = id_to_segment.find(segment->segmentId());
        if (it2 != id_to_segment.end())
        {
            LOG_DEBUG(log,
                      "Found segment with same id in id_to_segment: " << it2->second->info() << ", while my segment: " << segment->info());
        }
        return false;
    }
    auto & cur_segment = it->second;
    if (cur_segment.get() != segment.get())
    {
        LOG_DEBUG(log, "Segment [" << segment->segmentId() << "] instance has been replaced in segment map");
        return false;
    }
    return true;
}

void DeltaMergeStore::check(const Context & /*db_context*/)
{
    std::shared_lock lock(read_write_mutex);

    UInt64         next_segment_id = DELTA_MERGE_FIRST_SEGMENT_ID;
    RowKeyRange    last_range      = RowKeyRange::newAll(is_common_handle, rowkey_column_size);
    RowKeyValueRef last_end        = last_range.getStart();
    for (const auto & [end, segment] : segments)
    {
        (void)end;

        auto segment_id = segment->segmentId();
        auto range      = segment->getRowKeyRange();

        if (next_segment_id != segment_id)
        {
            String msg = "Check failed. Segments: ";
            for (auto & [end, segment] : segments)
            {
                (void)end;
                msg += segment->info() + ",";
            }
            msg.pop_back();
            msg += "}";
            LOG_ERROR(log, msg);

            throw Exception("Segment [" + DB::toString(segment_id) + "] is expected to have id [" + DB::toString(next_segment_id) + "]");
        }
        if (compare(last_end.data, last_end.size, range.getStart().data, range.getStart().size) != 0)
            throw Exception("Segment [" + DB::toString(segment_id) + ":" + range.toDebugString()
                            + "] is expected to have the same start edge value like the end edge value in " + last_range.toDebugString());

        last_range      = range;
        last_end        = last_range.getEnd();
        next_segment_id = segment->nextSegmentId();
    }
    if (!last_range.isEndInfinite())
        throw Exception("Last range " + last_range.toDebugString() + " is expected to have infinite end edge");
}

BlockPtr DeltaMergeStore::getHeader() const
{
    return std::atomic_load<Block>(&original_table_header);
}

void DeltaMergeStore::applyAlters(const AlterCommands &         commands,
                                  const OptionTableInfoConstRef table_info,
                                  ColumnID &                    max_column_id_used,
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
    LOG_DEBUG(log, "Loading dt files");

    DMFile::ListOptions options;
    options.only_list_can_gc = false; // We need all files to restore the bytes on disk
    options.clean_up         = true;
    auto file_provider       = global_context.getFileProvider();
    auto path_delegate       = path_pool.getStableDiskDelegator();
    for (const auto & root_path : path_delegate.listPaths())
    {
        for (auto & file_id : DMFile::listAllInPath(file_provider, root_path, options))
        {
            auto dmfile = DMFile::restore(file_provider, file_id, /* ref_id= */ 0, root_path, true);
            path_delegate.addDTFile(file_id, dmfile->getBytesOnDisk(), root_path);
        }
    }
}

DeltaMergeStoreStat DeltaMergeStore::getStat()
{
    std::shared_lock lock(read_write_mutex);

    DeltaMergeStoreStat stat;

    stat.segment_count = segments.size();

    long    total_placed_rows            = 0;
    long    total_delta_cache_rows       = 0;
    Float64 total_delta_cache_size       = 0;
    long    total_delta_valid_cache_rows = 0;
    for (const auto & [handle, segment] : segments)
    {
        (void)handle;
        auto & delta  = segment->getDelta();
        auto & stable = segment->getStable();

        total_placed_rows += delta->getPlacedDeltaRows();

        if (delta->getPackCount())
        {
            stat.total_rows += delta->getRows();
            stat.total_size += delta->getBytes();

            stat.total_delete_ranges += delta->getDeletes();

            stat.delta_count += 1;
            stat.total_pack_count_in_delta += delta->getPackCount();

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

    stat.delta_rate_rows     = (Float64)stat.total_delta_rows / stat.total_rows;
    stat.delta_rate_segments = (Float64)stat.delta_count / stat.segment_count;

    stat.delta_placed_rate       = (Float64)total_placed_rows / stat.total_delta_rows;
    stat.delta_cache_size        = total_delta_cache_size;
    stat.delta_cache_rate        = (Float64)total_delta_valid_cache_rows / stat.total_delta_rows;
    stat.delta_cache_wasted_rate = (Float64)(total_delta_cache_rows - total_delta_valid_cache_rows) / total_delta_valid_cache_rows;

    stat.avg_segment_rows = (Float64)stat.total_rows / stat.segment_count;
    stat.avg_segment_size = (Float64)stat.total_size / stat.segment_count;

    stat.avg_delta_rows          = (Float64)stat.total_delta_rows / stat.delta_count;
    stat.avg_delta_size          = (Float64)stat.total_delta_size / stat.delta_count;
    stat.avg_delta_delete_ranges = (Float64)stat.total_delete_ranges / stat.delta_count;

    stat.avg_stable_rows = (Float64)stat.total_stable_rows / stat.stable_count;
    stat.avg_stable_size = (Float64)stat.total_stable_size / stat.stable_count;

    stat.avg_pack_count_in_delta = (Float64)stat.total_pack_count_in_delta / stat.delta_count;
    stat.avg_pack_rows_in_delta  = (Float64)stat.total_delta_rows / stat.total_pack_count_in_delta;
    stat.avg_pack_size_in_delta  = (Float64)stat.total_delta_size / stat.total_pack_count_in_delta;

    stat.avg_pack_count_in_stable = (Float64)stat.total_pack_count_in_stable / stat.stable_count;
    stat.avg_pack_rows_in_stable  = (Float64)stat.total_stable_rows / stat.total_pack_count_in_stable;
    stat.avg_pack_size_in_stable  = (Float64)stat.total_stable_size / stat.total_pack_count_in_stable;

    {
        std::tie(stat.storage_stable_num_snapshots, //
                 stat.storage_stable_oldest_snapshot_lifetime,
                 stat.storage_stable_oldest_snapshot_thread_id)
            = storage_pool.data().getSnapshotsStat();
        PageStorage::SnapshotPtr stable_snapshot = storage_pool.data().getSnapshot();
        stat.storage_stable_num_pages            = stable_snapshot->version()->numPages();
        stat.storage_stable_num_normal_pages     = stable_snapshot->version()->numNormalPages();
        stat.storage_stable_max_page_id          = stable_snapshot->version()->maxId();
    }
    {
        std::tie(stat.storage_delta_num_snapshots, //
                 stat.storage_delta_oldest_snapshot_lifetime,
                 stat.storage_delta_oldest_snapshot_thread_id)
            = storage_pool.log().getSnapshotsStat();
        PageStorage::SnapshotPtr log_snapshot = storage_pool.log().getSnapshot();
        stat.storage_delta_num_pages          = log_snapshot->version()->numPages();
        stat.storage_delta_num_normal_pages   = log_snapshot->version()->numNormalPages();
        stat.storage_delta_max_page_id        = log_snapshot->version()->maxId();
    }
    {
        std::tie(stat.storage_meta_num_snapshots, //
                 stat.storage_meta_oldest_snapshot_lifetime,
                 stat.storage_meta_oldest_snapshot_thread_id)
            = storage_pool.meta().getSnapshotsStat();
        PageStorage::SnapshotPtr meta_snapshot = storage_pool.meta().getSnapshot();
        stat.storage_meta_num_pages            = meta_snapshot->version()->numPages();
        stat.storage_meta_num_normal_pages     = meta_snapshot->version()->numNormalPages();
        stat.storage_meta_max_page_id          = meta_snapshot->version()->maxId();
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
        auto &      delta  = segment->getDelta();
        auto &      stable = segment->getStable();

        stat.segment_id = segment->segmentId();
        stat.range      = segment->getRowKeyRange();

        stat.rows          = segment->getEstimatedRows();
        stat.size          = delta->getBytes() + stable->getBytes();
        stat.delete_ranges = delta->getDeletes();

        stat.stable_size_on_disk = stable->getBytesOnDisk();

        stat.delta_pack_count  = delta->getPackCount();
        stat.stable_pack_count = stable->getPacks();

        stat.avg_delta_pack_rows  = (Float64)delta->getRows() / stat.delta_pack_count;
        stat.avg_stable_pack_rows = (Float64)stable->getRows() / stat.stable_pack_count;

        stat.delta_rate       = (Float64)delta->getRows() / stat.rows;
        stat.delta_cache_size = delta->getTotalCacheBytes();

        stat.delta_index_size = delta->getDeltaIndexBytes();

        stats.push_back(stat);
    }
    return stats;
}

SegmentReadTasks DeltaMergeStore::getReadTasksByRanges(DMContext &          dm_context,
                                                       const RowKeyRanges & sorted_ranges,
                                                       size_t               expected_tasks_count,
                                                       const SegmentIdSet & read_segments)
{
    SegmentReadTasks tasks;

    std::shared_lock lock(read_write_mutex);

    auto range_it = sorted_ranges.begin();
    auto seg_it   = segments.upper_bound(range_it->getStart());

    if (seg_it == segments.end())
    {
        throw Exception("Failed to locate segment begin with start in range: " + range_it->toDebugString(), ErrorCodes::LOGICAL_ERROR);
    }

    while (range_it != sorted_ranges.end() && seg_it != segments.end())
    {
        auto & req_range = *range_it;
        auto & seg_range = seg_it->second->getRowKeyRange();
        if (req_range.intersect(seg_range) && (read_segments.empty() || read_segments.count(seg_it->second->segmentId())))
        {
            if (tasks.empty() || tasks.back()->segment != seg_it->second)
            {
                auto segment      = seg_it->second;
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

    LOG_DEBUG(log,
              __FUNCTION__ << " [sorted_ranges: " << sorted_ranges.size() << "] [tasks before split: " << tasks.size()
                           << "] [tasks final: " << result_tasks.size() << "] [ranges final: " << total_ranges << "]");

    return result_tasks;
}

} // namespace DM
} // namespace DB

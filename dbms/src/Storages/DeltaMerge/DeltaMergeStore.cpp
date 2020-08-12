#include <Columns/ColumnVector.h>
#include <Common/TiFlashMetrics.h>
#include <Common/typeid_cast.h>
#include <Core/SortDescription.h>
#include <Interpreters/sortBlock.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMSegmentThreadInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
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

// ================================================
//   MergeDeltaTaskPool
// ================================================

void DeltaMergeStore::MergeDeltaTaskPool::addTask(const BackgroundTask & task, const ThreadType & whom, Logger * log_)
{
    LOG_DEBUG(log_,
              "Segment [" << task.segment->segmentId() << "] task [" << toString(task.type) << "] add to background task pool by ["
                          << toString(whom) << "]");

    std::scoped_lock lock(mutex);
    tasks.push(task);
}

DeltaMergeStore::BackgroundTask DeltaMergeStore::MergeDeltaTaskPool::nextTask(Logger * log_)
{
    std::scoped_lock lock(mutex);

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
ColumnDefinesPtr getStoreColumns(const ColumnDefines & table_columns)
{
    auto columns = std::make_shared<ColumnDefines>();
    // First three columns are always _tidb_rowid, _INTERNAL_VERSION, _INTERNAL_DELMARK
    columns->emplace_back(getExtraHandleColumnDefine());
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

DeltaMergeStore::DeltaMergeStore(Context &             db_context,
                                 const String &        path_,
                                 bool                  data_path_contains_database_name,
                                 const String &        db_name_,
                                 const String &        table_name_,
                                 const ColumnDefines & columns,
                                 const ColumnDefine &  handle,
                                 const Settings &      settings_)
    : path(path_),
      global_context(db_context.getGlobalContext()),
      settings(settings_),
      storage_pool(db_name_ + "." + table_name_, path, global_context, db_context.getSettingsRef()),
      db_name(db_name_),
      table_name(table_name_),
      original_table_handle_define(handle),
      background_pool(db_context.getBackgroundPool()),
      hash_salt(++DELTA_MERGE_STORE_HASH_SALT),
      log(&Logger::get("DeltaMergeStore[" + db_name + "." + table_name + "]"))
{
    LOG_INFO(log, "Restore DeltaMerge Store start [" << db_name << "." << table_name << "]");

    auto & extra_paths_root = global_context.getExtraPaths();
    extra_paths             = extra_paths_root.withTable(db_name, table_name_, data_path_contains_database_name);
    // restore existing dm files and set capacity for extra_paths.
    restoreExtraPathCapacity();

    original_table_columns.emplace_back(original_table_handle_define);
    original_table_columns.emplace_back(getVersionColumnDefine());
    original_table_columns.emplace_back(getTagColumnDefine());
    for (const auto & col : columns)
    {
        if (col.id != original_table_handle_define.id && col.id != VERSION_COLUMN_ID && col.id != TAG_COLUMN_ID)
            original_table_columns.emplace_back(col);
    }

    original_table_header = std::make_shared<Block>(toEmptyBlock(original_table_columns));
    store_columns         = getStoreColumns(original_table_columns);

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
            auto first_segment = Segment::newSegment(*dm_context, HandleRange::newAll(), segment_id, 0);
            segments.emplace(first_segment->getRange().end, first_segment);
            id_to_segment.emplace(segment_id, first_segment);
        }
        else
        {
            auto segment_id = DELTA_MERGE_FIRST_SEGMENT_ID;
            while (segment_id)
            {
                auto segment = Segment::restoreSegment(*dm_context, segment_id);
                segments.emplace(segment->getRange().end, segment);
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
        for (auto & root_path : extra_paths.listPaths())
        {
            auto & path_and_ids           = path_and_ids_vec.emplace_back();
            path_and_ids.first            = root_path;
            auto file_ids_in_current_path = DMFile::listAllInPath(root_path + "/" + STABLE_FOLDER_NAME, /* can_gc= */ true);
            for (auto id : file_ids_in_current_path)
                path_and_ids.second.insert(id);
        }
        return path_and_ids_vec;
    };
    auto dmfile_remover = [&](const PageStorage::PathAndIdsVec & path_and_ids_vec, const std::set<PageId> & valid_ids) {
        for (auto & [path, ids] : path_and_ids_vec)
        {
            for (auto id : ids)
            {
                if (valid_ids.count(id))
                    continue;

                // Note that ref_id is useless here.
                auto dmfile = DMFile::restore(global_context.getFileProvider(), id, /* ref_id= */ 0, path + "/" + STABLE_FOLDER_NAME, false);
                if (dmfile->canGC())
                {
                    extra_paths.removeDMFile(dmfile->fileId());
                    dmfile->remove(global_context.getFileProvider());
                }

                LOG_DEBUG(log, "GC removed useless dmfile: " << dmfile->path());
            }
        }
    };
    storage_pool.data().registerExternalPagesCallbacks(dmfile_scanner, dmfile_remover);

    gc_handle              = background_pool.addTask([this] { return storage_pool.gc(); });
    background_task_handle = background_pool.addTask([this] { return handleBackgroundTask(); });

    // Do place delta index.
    for (auto & [end, segment] : segments)
    {
        (void)end;
        checkSegmentUpdate(dm_context, segment, ThreadType::Init);
    }

    // Wake up to do place delta index tasks.
    background_task_handle->wake();
}

void DeltaMergeStore::rename(String new_path, bool clean_rename, String new_database_name, String new_table_name)
{
    if (clean_rename)
    {
        extra_paths.rename(new_database_name, new_table_name, clean_rename);
    }
    else
    {
        LOG_WARNING(log,
                    "Applying heavy renaming for table " << db_name << "." << table_name //
                                                         << " to " << new_database_name << "." << new_table_name);

        // Remove all background task first
        shutdown();
        extra_paths.rename(new_database_name, new_table_name, clean_rename); // rename for multi-disk
        // Check if path is covered by extra_paths, if not, rename
        if (auto dir = Poco::File(path); dir.exists())
        {
            LOG_INFO(log, "Renaming " << path << " to " << new_path);
            dir.renameTo(new_path);
        }
        // setting `path` is useless, we need to restore the whole DeltaMergeStore object after path is changed.
        // path = new_path;
    }

    // TODO: replacing these two variables is not atomic, but could be good enough?
    table_name.swap(new_table_name);
    db_name.swap(new_database_name);
}

void DeltaMergeStore::drop()
{
    // Remove all background task first
    shutdown();
    storage_pool.drop();
    // Drop data in extra path (stable data by default)
    extra_paths.drop(true);
    // Check if path(delta && meta by default) is covered by extra_paths, if not, drop it.
    Poco::File dir(path);
    if (dir.exists())
        dir.remove(true);

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

    LOG_DEBUG(log, "Shutdown DeltaMerge Store start [" << db_name << "." << table_name << "]");
    background_pool.removeTask(gc_handle);
    gc_handle = nullptr;

    background_pool.removeTask(background_task_handle);
    background_task_handle = nullptr;
    LOG_DEBUG(log, "Shutdown DeltaMerge Store start [" << db_name << "." << table_name << "]");
}

DMContextPtr DeltaMergeStore::newDMContext(const Context & db_context, const DB::Settings & db_settings)
{
    std::shared_lock lock(read_write_mutex);

    // Here we use global context from db_context, instead of db_context directly.
    // Because db_context could be a temporary object and won't last long enough during the query process.
    // Like the context created by InterpreterSelectWithUnionQuery.
    auto * ctx = new DMContext(db_context.getGlobalContext(),
                               path,
                               extra_paths,
                               storage_pool,
                               hash_salt,
                               store_columns,
                               latest_gc_safe_point,
                               settings.not_compress_columns,
                               db_settings);
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

void DeltaMergeStore::write(const Context & db_context, const DB::Settings & db_settings, const Block & to_write)
{
    LOG_TRACE(log, "Write into " << db_name << "." << table_name << " " << to_write.rows() << " rows.");

    EventRecorder write_block_recorder(ProfileEvents::DMWriteBlock, ProfileEvents::DMWriteBlockNS);

    const size_t rows = to_write.rows();
    if (rows == 0)
        return;

    auto  dm_context = newDMContext(db_context, db_settings);
    Block block      = to_write;

    // Add an extra handle column, if handle reused the original column data.
    if (pkIsHandle())
    {
        auto handle_pos = getPosByColumnId(block, original_table_handle_define.id);
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

        if (rows > 1 && !isAlreadySorted(block, sort))
            stableSortBlock(block, sort);
    }

    if (false && log->trace())
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

    Segments updated_segments;

    size_t       offset = 0;
    size_t       limit;
    const auto & handle_data = getColumnVectorData<Handle>(block, block.getPositionByName(EXTRA_HANDLE_COLUMN_NAME));
    while (offset != rows)
    {
        auto start_handle = handle_data[offset];

        WriteBatches wbs(storage_pool);
        PackPtr      write_pack;
        HandleRange  write_range;

        // Keep trying until succeeded.
        while (true)
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(start_handle);
                if (segment_it == segments.end())
                {
                    if (start_handle == P_INF_HANDLE)
                        --segment_it;
                    else
                        throw Exception("Failed to locate segment begin with start: " + DB::toString(start_handle),
                                        ErrorCodes::LOGICAL_ERROR);
                }
                segment = segment_it->second;
            }

            waitForWrite(dm_context, segment);

            auto range   = segment->getRange();
            auto end_pos = range.end == P_INF_HANDLE ? handle_data.cend()
                                                     : std::lower_bound(handle_data.cbegin() + offset, handle_data.cend(), range.end);
            limit = end_pos - (handle_data.cbegin() + offset);

            bool should_cache = limit < dm_context->delta_cache_limit_rows / 4;
            if (should_cache)
            {
                if (segment->writeToCache(*dm_context, block, offset, limit))
                {
                    updated_segments.push_back(segment);
                    break;
                }
            }
            else
            {
                if (!write_pack || (write_pack && write_range != range))
                {
                    wbs.rollbackWrittenLogAndData();
                    wbs.clear();

                    write_pack = DeltaValueSpace::writePack(*dm_context, block, offset, limit, wbs);
                    wbs.writeLogAndData();
                    write_range = range;
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

    if (db_settings.dt_flush_after_write)
    {
        HandleRange merge_range = HandleRange::newNone();
        for (auto & segment : updated_segments)
            merge_range = merge_range.merge(segment->getRange());
        flushCache(dm_context, merge_range);
    }

    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);
}


void DeltaMergeStore::deleteRange(const Context & db_context, const DB::Settings & db_settings, const HandleRange & delete_range)
{
    LOG_INFO(log, "Write into " << db_name << "." << table_name << " delte range " << delete_range.toString());

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

    Segments updated_segments;

    auto start_handle = delete_range.start;
    while (start_handle < delete_range.end)
    {
        Handle end_handle;
        // Keep trying until succeeded.
        while (true)
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(start_handle);
                if (segment_it == segments.end())
                {
                    if (start_handle == P_INF_HANDLE)
                        --segment_it;
                    else
                        throw Exception("Failed to locate segment begin with start: " + DB::toString(start_handle),
                                        ErrorCodes::LOGICAL_ERROR);
                }
                segment = segment_it->second;
            }

            waitForDeleteRange(dm_context, segment);

            auto range = segment->getRange();
            end_handle = range.end;

            // Write could fail, because other threads could already updated the instance. Like split/merge, merge delta.
            if (segment->write(*dm_context, delete_range.shrink(range)))
            {
                updated_segments.push_back(segment);
                break;
            }
        }

        start_handle = end_handle;
    }

    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);
}

void DeltaMergeStore::flushCache(const DMContextPtr & dm_context, const HandleRange & range)
{
    auto start_handle = range.start;
    while (start_handle < range.end)
    {
        Handle end_handle;
        // Keep trying until succeeded.
        while (true)
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(start_handle);
                if (segment_it == segments.end())
                {
                    if (start_handle == P_INF_HANDLE)
                        --segment_it;
                    else
                        throw Exception("Failed to locate segment begin with start: " + DB::toString(start_handle),
                                        ErrorCodes::LOGICAL_ERROR);
                }
                segment = segment_it->second;
            }
            auto seg_range = segment->getRange();
            end_handle     = seg_range.end;

            // Flush could fail.
            if (segment->flushCache(*dm_context))
            {
                break;
            }
        }

        start_handle = end_handle;
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
        segmentMergeDelta(*dm_context, segment, true);
    }
}

void DeltaMergeStore::compact(const Context & db_context, const HandleRange & range)
{
    auto dm_context = newDMContext(db_context, db_context.getSettingsRef());

    auto start_handle = range.start;
    while (start_handle < range.end)
    {
        Handle end_handle;
        // Keep trying until succeeded.
        while (true)
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(start_handle);
                if (segment_it == segments.end())
                {
                    if (start_handle == P_INF_HANDLE)
                        --segment_it;
                    else
                        throw Exception("Failed to locate segment begin with start: " + DB::toString(start_handle),
                                        ErrorCodes::LOGICAL_ERROR);
                }
                segment = segment_it->second;
            }
            auto seg_range = segment->getRange();
            end_handle     = seg_range.end;

            // compact could fail.
            if (segment->compactDelta(*dm_context))
            {
                break;
            }
        }

        start_handle = end_handle;
    }
}

BlockInputStreams DeltaMergeStore::readRaw(const Context &       db_context,
                                           const DB::Settings &  db_settings,
                                           const ColumnDefines & columns_to_read,
                                           size_t                num_streams,
                                           const SegmentIdSet &  read_segments)
{
    SegmentReadTasks tasks;

    auto dm_context = newDMContext(db_context, db_settings);

    {
        std::shared_lock lock(read_write_mutex);

        for (const auto & [handle, segment] : segments)
        {
            (void)handle;
            if (read_segments.empty() || read_segments.count(segment->segmentId()))
            {
                auto segment_snap = segment->createSnapshot(*dm_context);
                if (unlikely(!segment_snap))
                    throw Exception("Failed to get segment snap", ErrorCodes::LOGICAL_ERROR);
                tasks.push(std::make_shared<SegmentReadTask>(segment, segment_snap, HandleRanges{segment->getRange()}));
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
                                        const HandleRanges &  sorted_ranges,
                                        size_t                num_streams,
                                        UInt64                max_version,
                                        const RSOperatorPtr & filter,
                                        size_t                expected_block_size,
                                        const SegmentIdSet &  read_segments)
{
    LOG_DEBUG(log, "Read with " << sorted_ranges.size() << " ranges");

    SegmentReadTasks tasks;

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
            if (req_range.intersect(seg_range) && (read_segments.empty() || read_segments.count(seg_it->second->segmentId())))
            {
                if (tasks.empty() || tasks.back()->segment != seg_it->second)
                {
                    auto segment      = seg_it->second;
                    auto segment_snap = segment->createSnapshot(*dm_context);
                    if (unlikely(!segment_snap))
                        throw Exception("Failed to get segment snap", ErrorCodes::LOGICAL_ERROR);
                    tasks.push(std::make_shared<SegmentReadTask>(segment, segment_snap));
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

    LOG_DEBUG(log, "Read create segment snapshot done");

#if 0
    if (log->trace())
    {
        auto ranges_to_string = [](const HandleRanges & ranges) -> String {
            std::stringstream ss;
            bool              is_first = true;
            ss << "[";
            for (const auto & range : ranges)
            {
                if (!is_first)
                    ss << ",";
                is_first = false;
                ss << range.toString();
            }
            ss << "]";
            return ss.str();
        };
        for (const auto & task : tasks)
        {
            LOG_TRACE(log,
                      "Read range: " << ranges_to_string(sorted_ranges) << " -> segment: " << task->segment->info()
                                     << " range: " << ranges_to_string(task->ranges));
        }
    }
#endif

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
    return dm_context->segment_limit_rows;
}

size_t forceMergeDeltaDeletes(const DMContextPtr &)
{
    return 5;
}

void DeltaMergeStore::waitForWrite(const DMContextPtr & dm_context, const SegmentPtr & segment)
{
    size_t delta_limit_rows = dm_context->delta_limit_rows;
    size_t delta_rows       = segment->getDelta()->getRows();
    if (delta_rows < forceMergeDeltaRows(dm_context))
        return;

    size_t sleep_step = 1;
    size_t sleep_ms   = (double)delta_rows / delta_limit_rows * sleep_step;

    // checkSegmentUpdate could do foreground merge delta, so call it before sleep.
    checkSegmentUpdate(dm_context, segment, ThreadType::Write);

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

    size_t delta_saved_rows = delta->getRows(/* use_unsaved */ false);
    size_t delta_check_rows = std::max(delta->updatesInDeltaTree(), delta_saved_rows);

    size_t delta_deletes = delta->getDeletes();

    size_t unsaved_rows = delta->getUnsavedRows();

    size_t delta_rows   = delta_saved_rows + unsaved_rows;
    size_t segment_rows = segment->getEstimatedRows();
    size_t pack_count   = delta->getPackCount();

    size_t placed_delta_rows = delta->getPlacedDeltaRows();

    auto & delta_last_try_flush_rows             = delta->getLastTryFlushRows();
    auto & delta_last_try_compact_packs          = delta->getLastTryCompactPacks();
    auto & delta_last_try_merge_delta_rows       = delta->getLastTryMergeDeltaRows();
    auto & delta_last_try_split_rows             = delta->getLastTrySplitRows();
    auto & delta_last_try_place_delta_index_rows = delta->getLastTryPlaceDeltaIndexRows();

    auto segment_limit_rows     = dm_context->segment_limit_rows;
    auto delta_limit_rows       = dm_context->delta_limit_rows;
    auto delta_cache_limit_rows = dm_context->delta_cache_limit_rows;

    bool should_background_flush
        = unsaved_rows >= delta_cache_limit_rows && delta_rows - delta_last_try_flush_rows >= delta_cache_limit_rows;
    bool should_foreground_flush = unsaved_rows >= delta_cache_limit_rows * 3;

    bool should_background_merge_delta = (delta_check_rows >= delta_limit_rows //
                                          && delta_rows - delta_last_try_merge_delta_rows >= delta_cache_limit_rows)
        || delta_deletes >= 2;
    bool should_foreground_merge_delta
        = delta_check_rows >= forceMergeDeltaRows(dm_context) || delta_deletes >= forceMergeDeltaDeletes(dm_context);

    bool should_split = segment_rows >= segment_limit_rows * 2 && delta_rows - delta_last_try_split_rows >= delta_cache_limit_rows;
    bool should_merge = segment_rows < segment_limit_rows / 3;

    bool should_compact = std::max((Int64)pack_count - delta_last_try_compact_packs, 0) >= 10;

    bool should_place_delta_index = delta_rows - placed_delta_rows >= delta_cache_limit_rows * 3
        && delta_rows - delta_last_try_place_delta_index_rows >= delta_cache_limit_rows;

    auto try_add_background_task = [&](const BackgroundTask & task) {
        // Prevent too many tasks.
        if (background_tasks.length() <= std::max(id_to_segment.size() * 2, background_pool.getNumberOfThreads() * 3))
        {
            if (shutdown_called.load(std::memory_order_relaxed))
                return;

            background_tasks.addTask(task, thread_type, log);
            background_task_handle->wake();
        }
    };

    /// Flush is always try first.
    if (thread_type != ThreadType::Read)
    {
        if (should_foreground_flush)
        {
            delta_last_try_flush_rows = delta_rows;
            LOG_DEBUG(log, "Foreground flush cache " << segment->info());
            segment->flushCache(*dm_context);
        }
        else if (should_background_flush)
        {
            delta_last_try_flush_rows = delta_rows;
            try_add_background_task(BackgroundTask{TaskType::Flush, dm_context, segment, {}});
        }
    }

    if (segment->getDelta()->isUpdating())
        return;

    /// Now start trying structure update.

    auto getMergeSibling = [&]() -> SegmentPtr {
        /// For complexity reason, currently we only try to merge with next segment. Normally it is good enough.

        // The last segment cannot be merged.
        if (segment->getRange().end == P_INF_HANDLE)
            return {};
        SegmentPtr next_segment;
        {
            std::shared_lock read_write_lock(read_write_mutex);

            auto it = segments.find(segment->getRange().end);
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
    SegmentPtr merge_sibling;

    auto try_fg_merge_delta = [&]() -> SegmentPtr {
        if (should_foreground_merge_delta)
        {
            delta_last_try_merge_delta_rows = delta_rows;
            return segmentMergeDelta(*dm_context, segment, true);
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
        if (should_split)
        {
            delta_last_try_split_rows = delta_rows;
            try_add_background_task(BackgroundTask{TaskType::Split, dm_context, seg, {}});
            return true;
        }
        return false;
    };
    auto try_fg_split = [&](const SegmentPtr & my_segment) -> bool {
        auto my_segment_rows = my_segment->getEstimatedRows();
        auto my_should_split = my_segment_rows >= dm_context->segment_limit_rows * 3;
        if (my_should_split)
        {
            if (segmentSplit(*dm_context, my_segment).first)
                return true;
            else
                return false;
        }
        return false;
    };
    auto try_bg_merge = [&]() {
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

        SegmentPtr new_segment;
        if ((new_segment = try_fg_merge_delta()))
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

bool DeltaMergeStore::handleBackgroundTask()
{
    auto task = background_tasks.nextTask(log);
    if (!task)
        return false;

    // Update GC safe point before background task
    /// Note that `task.dm_context->db_context` will be free after query is finish. We should not use that in background task.
    auto pd_client = global_context.getTMTContext().getPDClient();
    if (!pd_client->isMock())
    {
        auto safe_point = PDClientHelper::getGCSafePointWithRetry(pd_client,
                                                                  /* ignore_cache= */ false,
                                                                  global_context.getSettingsRef().safe_point_update_interval_seconds);

        LOG_DEBUG(log, "Task" << toString(task.type) << " GC safe point: " << safe_point);

        // Foreground task don't get GC safe point from remote, but we better make it as up to date as possible.
        latest_gc_safe_point         = safe_point;
        task.dm_context->min_version = safe_point;
    }

    SegmentPtr left, right;
    ThreadType type = ThreadType::Write;
    try
    {
        switch (task.type)
        {
        case Split:
            std::tie(left, right) = segmentSplit(*task.dm_context, task.segment);
            type                  = ThreadType::BG_Split;
            break;
        case Merge:
            segmentMerge(*task.dm_context, task.segment, task.next_segment);
            type = ThreadType::BG_Merge;
            break;
        case MergeDelta:
            left = segmentMergeDelta(*task.dm_context, task.segment, false);
            type = ThreadType::BG_MergeDelta;
            break;
        case Compact: {
            task.segment->compactDelta(*task.dm_context);
            left = task.segment;
            type = ThreadType::BG_Compact;
            break;
        }
        case Flush: {
            task.segment->flushCache(*task.dm_context);
            // After flush cache, better place delta index.
            task.segment->placeDeltaIndex(*task.dm_context);
            left = task.segment;
            type = ThreadType::BG_Flush;
            break;
        }
        case PlaceIndex: {
            task.segment->placeDeltaIndex(*task.dm_context);
            break;
        }
        default:
            throw Exception("Unsupported task type: " + DeltaMergeStore::toString(task.type));
        }
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log,
                  "Task " << toString(task.type) << " on Segment [" << task.segment->segmentId()
                          << ((bool)task.next_segment ? ("] and [" + DB::toString(task.next_segment->segmentId())) : "")
                          << "] failed. Error msg: " << e.message());
        e.rethrow();
    }

    if (left)
        checkSegmentUpdate(task.dm_context, left, type);
    if (right)
        checkSegmentUpdate(task.dm_context, right, type);

    return true;
}

SegmentPair DeltaMergeStore::segmentSplit(DMContext & dm_context, const SegmentPtr & segment)
{
    LOG_DEBUG(log, "Split segment " << segment->info() << ", safe point:" << dm_context.min_version);

    SegmentSnapshotPtr segment_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(segment))
        {
            LOG_DEBUG(log, "Give up segment [" << segment->segmentId() << "] split");
            return {};
        }

        segment_snap = segment->createSnapshot(dm_context, /* is_update */ true);
        if (!segment_snap)
        {
            LOG_DEBUG(log, "Give up segment [" << segment->segmentId() << "] split");
            return {};
        }
    }

    // Not counting the early give up action.
    GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_seg_split).Increment();
    Stopwatch watch_seg_split;
    SCOPE_EXIT({
        GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_seg_split).Observe(watch_seg_split.elapsedSeconds());
    });

    WriteBatches wbs(storage_pool);
    auto         range      = segment->getRange();
    auto         split_info = segment->prepareSplit(dm_context, segment_snap, wbs);

    wbs.writeLogAndData();
    split_info.my_stable->enableDMFilesGC();
    split_info.other_stable->enableDMFilesGC();

    SegmentPtr new_left, new_right;
    {
        std::unique_lock lock(read_write_mutex);

        if (!isSegmentValid(segment))
        {
            LOG_DEBUG(log, "Give up segment [" << segment->segmentId() << "] split");
            wbs.setRollback();
            return {};
        }

        LOG_DEBUG(log, "Apply split. Segment [" << segment->segmentId() << "]");

        auto segment_lock = segment->mustGetUpdateLock();

        std::tie(new_left, new_right) = segment->applySplit(dm_context, segment_snap, wbs, split_info);

        wbs.writeMeta();

        segment->abandon();
        segments.erase(range.end);
        id_to_segment.erase(segment->segmentId());

        segments[new_left->getRange().end]  = new_left;
        segments[new_right->getRange().end] = new_right;

        id_to_segment.emplace(new_left->segmentId(), new_left);
        id_to_segment.emplace(new_right->segmentId(), new_right);

        if constexpr (DM_RUN_CHECK)
        {
            new_left->check(dm_context, "After split left");
            new_right->check(dm_context, "After split right");
        }

        LOG_DEBUG(log, "Apply split done. Segment [" << segment->segmentId() << "]");
    }

    wbs.writeRemoves();

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);

    return {new_left, new_right};
}

void DeltaMergeStore::segmentMerge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right)
{
    LOG_DEBUG(log, "Merge Segment [" << left->info() << "] and [" << right->info() << "], safe point:" << dm_context.min_version);

    SegmentSnapshotPtr left_snap;
    SegmentSnapshotPtr right_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(left))
        {
            LOG_DEBUG(log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");
            return;
        }
        if (!isSegmentValid(right))
        {
            LOG_DEBUG(log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");
            return;
        }

        left_snap  = left->createSnapshot(dm_context, /* is_update */ true);
        right_snap = right->createSnapshot(dm_context, /* is_update */ true);

        if (!left_snap || !right_snap)
        {
            LOG_DEBUG(log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");
            return;
        }
    }

    // Not counting the early give up action.
    GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_seg_merge).Increment();
    Stopwatch watch_seg_merge;
    SCOPE_EXIT({
        GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_seg_merge).Observe(watch_seg_merge.elapsedSeconds());
    });

    auto left_range  = left->getRange();
    auto right_range = right->getRange();

    WriteBatches wbs(storage_pool);
    auto         merged_stable = Segment::prepareMerge(dm_context, left, left_snap, right, right_snap, wbs);
    wbs.writeLogAndData();
    merged_stable->enableDMFilesGC();

    {
        std::unique_lock lock(read_write_mutex);

        if (!isSegmentValid(left) || !isSegmentValid(right))
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

        left->abandon();
        right->abandon();
        segments.erase(left_range.end);
        segments.erase(right_range.end);
        id_to_segment.erase(left->segmentId());
        id_to_segment.erase(right->segmentId());

        segments.emplace(merged->getRange().end, merged);
        id_to_segment.emplace(merged->segmentId(), merged);

        if constexpr (DM_RUN_CHECK)
        {
            merged->check(dm_context, "After segment merge");
        }

        LOG_DEBUG(log, "Apply merge done. [" << left->info() << "] and [" << right->info() << "]");
    }

    wbs.writeRemoves();

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);
}

SegmentPtr DeltaMergeStore::segmentMergeDelta(DMContext & dm_context, const SegmentPtr & segment, bool is_foreground)
{
    LOG_DEBUG(log,
              (is_foreground ? "Foreground" : "Background")
                  << " merge delta, segment [" << segment->segmentId() << "], safe point:" << dm_context.min_version);

    SegmentSnapshotPtr segment_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(segment))
        {
            LOG_DEBUG(log, "Give up merge delta, segment [" << segment->segmentId() << "]");
            return {};
        }

        segment_snap = segment->createSnapshot(dm_context, /* is_update */ true);
        if (!segment_snap)
        {
            LOG_DEBUG(log, "Give up merge delta, segment [" << segment->segmentId() << "]");
            return {};
        }
    }

    // Not counting the early give up action.
    GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_delta_merge).Increment();
    Stopwatch watch_delta_merge;
    SCOPE_EXIT({
        GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_delta_merge)
            .Observe(watch_delta_merge.elapsedSeconds());
    });

    WriteBatches wbs(storage_pool);

    auto new_stable = segment->prepareMergeDelta(dm_context, segment_snap, wbs);
    wbs.writeLogAndData();
    new_stable->enableDMFilesGC();

    SegmentPtr new_segment;
    {
        std::unique_lock read_write_lock(read_write_mutex);

        if (!isSegmentValid(segment))
        {
            LOG_DEBUG(log, "Give up merge delta, segment [" << segment->segmentId() << "]");
            wbs.setRollback();
            return {};
        }

        LOG_DEBUG(log, "Apply merge delta. Segment [" << segment->info() << "]");

        auto segment_lock = segment->mustGetUpdateLock();

        new_segment = segment->applyMergeDelta(dm_context, segment_snap, wbs, new_stable);

        wbs.writeMeta();

        segments[segment->getRange().end]   = new_segment;
        id_to_segment[segment->segmentId()] = new_segment;

        segment->abandon();

        if constexpr (DM_RUN_CHECK)
        {
            new_segment->check(dm_context, "After merge delta");
        }

        LOG_DEBUG(log, "Apply merge delta done. Segment [" << segment->segmentId() << "]");
    }

    wbs.writeRemoves();

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);

    return new_segment;
}

bool DeltaMergeStore::isSegmentValid(const SegmentPtr & segment)
{
    if (segment->hasAbandoned())
        return false;
    // Segment instance could have been removed or replaced.
    auto it = segments.find(segment->getRange().end);
    if (it == segments.end())
        return false;
    auto & cur_segment = it->second;
    return cur_segment.get() == segment.get();
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

BlockPtr DeltaMergeStore::getHeader() const
{
    return std::atomic_load<Block>(&original_table_header);
};

void DeltaMergeStore::applyAlters(const AlterCommands &         commands,
                                  const OptionTableInfoConstRef table_info,
                                  ColumnID &                    max_column_id_used,
                                  const Context & /* context */)
{
    std::unique_lock lock(read_write_mutex);

    ColumnDefines new_original_table_columns(original_table_columns.begin(), original_table_columns.end());
    for (const auto & command : commands)
    {
        applyAlter(new_original_table_columns, command, table_info, max_column_id_used);
    }

    if (table_info)
    {
        // Update primary keys from TiDB::TableInfo

        // For TiDB 3.1/4.0, there should be only one column with pri key flag.
        // FIXME: With feature clustered index in TiDB 5.0, there could be multiple columns with primary key flag
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

    auto new_store_columns = getStoreColumns(new_original_table_columns);

    original_table_columns.swap(new_original_table_columns);
    store_columns.swap(new_store_columns);

    std::atomic_store<Block>(&original_table_header, std::make_shared<Block>(toEmptyBlock(original_table_columns)));
}


SortDescription DeltaMergeStore::getPrimarySortDescription() const
{
    std::shared_lock lock(read_write_mutex);

    SortDescription desc;
    desc.emplace_back(original_table_handle_define.name, /* direction_= */ 1, /* nulls_direction_= */ 1);
    return desc;
}

void DeltaMergeStore::restoreExtraPathCapacity()
{
    LOG_DEBUG(log, "Loading dm files");

    for (const auto & root_path : extra_paths.listPaths())
    {
        auto parent_path = root_path + "/" + STABLE_FOLDER_NAME;
        for (auto & file_id : DMFile::listAllInPath(parent_path, false))
        {
            auto dmfile = DMFile::restore(global_context.getFileProvider(), file_id, /* ref_id= */ 0, parent_path, true);
            extra_paths.addDMFile(file_id, dmfile->getBytesOnDisk(), root_path);
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
        stat.storage_stable_num_snapshots        = storage_pool.data().getNumSnapshots();
        PageStorage::SnapshotPtr stable_snapshot = storage_pool.data().getSnapshot();
        stat.storage_stable_num_pages            = stable_snapshot->version()->numPages();
        stat.storage_stable_num_normal_pages     = stable_snapshot->version()->numNormalPages();
        stat.storage_stable_max_page_id          = stable_snapshot->version()->maxId();
    }
    {
        stat.storage_delta_num_snapshots      = storage_pool.log().getNumSnapshots();
        PageStorage::SnapshotPtr log_snapshot = storage_pool.log().getSnapshot();
        stat.storage_delta_num_pages          = log_snapshot->version()->numPages();
        stat.storage_delta_num_normal_pages   = log_snapshot->version()->numNormalPages();
        stat.storage_delta_max_page_id        = log_snapshot->version()->maxId();
    }
    {
        stat.storage_meta_num_snapshots        = storage_pool.meta().getNumSnapshots();
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
        stat.range      = segment->getRange();

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

} // namespace DM
} // namespace DB

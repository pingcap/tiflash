// Copyright 2022 PingCAP, Ltd.
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

#pragma once

#include <Common/UniThreadPool.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context_fwd.h>
#include <Operators/Operator.h>
#include <Storages/AlterCommands.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot_fwd.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Page/PageStorage_fwd.h>
#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>
#include <Storages/Transaction/TiDB.h>

#include <queue>

namespace DB
{

class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
struct CheckpointInfo;
using CheckpointInfoPtr = std::shared_ptr<CheckpointInfo>;

class StoragePathPool;

class PipelineExecutorStatus;
class PipelineExecGroupBuilder;

namespace DM
{
class StoragePool;
using StoragePoolPtr = std::shared_ptr<StoragePool>;
class DMFile;
using DMFilePtr = std::shared_ptr<DMFile>;
class Segment;
using SegmentPtr = std::shared_ptr<Segment>;
using SegmentPair = std::pair<SegmentPtr, SegmentPtr>;
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;
struct DMContext;
using DMContextPtr = std::shared_ptr<DMContext>;
using NotCompress = std::unordered_set<ColId>;
using SegmentIdSet = std::unordered_set<UInt64>;
struct ExternalDTFileInfo;
struct GCOptions;

namespace tests
{
class DeltaMergeStoreTest;
}

inline static const PageIdU64 DELTA_MERGE_FIRST_SEGMENT_ID = 1;

struct SegmentStats
{
    UInt64 segment_id = 0;
    RowKeyRange range;
    UInt64 epoch = 0;
    UInt64 rows = 0;
    UInt64 size = 0;

    Float64 delta_rate = 0;
    UInt64 delta_memtable_rows = 0;
    UInt64 delta_memtable_size = 0;
    UInt64 delta_memtable_column_files = 0;
    UInt64 delta_memtable_delete_ranges = 0;
    UInt64 delta_persisted_page_id = 0;
    UInt64 delta_persisted_rows = 0;
    UInt64 delta_persisted_size = 0;
    UInt64 delta_persisted_column_files = 0;
    UInt64 delta_persisted_delete_ranges = 0;
    UInt64 delta_cache_size = 0;
    UInt64 delta_index_size = 0;

    UInt64 stable_page_id = 0;
    UInt64 stable_rows = 0;
    UInt64 stable_size = 0;
    UInt64 stable_dmfiles = 0;
    UInt64 stable_dmfiles_id_0 = 0;
    UInt64 stable_dmfiles_rows = 0;
    UInt64 stable_dmfiles_size = 0;
    UInt64 stable_dmfiles_size_on_disk = 0;
    UInt64 stable_dmfiles_packs = 0;
};
using SegmentsStats = std::vector<SegmentStats>;

struct StoreStats
{
    UInt64 segment_count = 0;

    UInt64 total_rows = 0;
    UInt64 total_size = 0;
    UInt64 total_delete_ranges = 0;

    Float64 delta_rate_rows = 0;
    Float64 delta_rate_segments = 0;

    Float64 delta_placed_rate = 0;
    UInt64 delta_cache_size = 0;
    Float64 delta_cache_rate = 0;
    Float64 delta_cache_wasted_rate = 0;

    UInt64 delta_index_size = 0;

    Float64 avg_segment_rows = 0;
    Float64 avg_segment_size = 0;

    UInt64 delta_count = 0;
    UInt64 total_delta_rows = 0;
    UInt64 total_delta_size = 0;
    Float64 avg_delta_rows = 0;
    Float64 avg_delta_size = 0;
    Float64 avg_delta_delete_ranges = 0;

    UInt64 stable_count = 0;
    UInt64 total_stable_rows = 0;
    UInt64 total_stable_size = 0;
    UInt64 total_stable_size_on_disk = 0;
    Float64 avg_stable_rows = 0;
    Float64 avg_stable_size = 0;

    // statistics about column file in delta
    UInt64 total_pack_count_in_delta = 0;
    UInt64 max_pack_count_in_delta = 0;
    Float64 avg_pack_count_in_delta = 0;
    Float64 avg_pack_rows_in_delta = 0;
    Float64 avg_pack_size_in_delta = 0;

    UInt64 total_pack_count_in_stable = 0;
    Float64 avg_pack_count_in_stable = 0;
    Float64 avg_pack_rows_in_stable = 0;
    Float64 avg_pack_size_in_stable = 0;

    UInt64 storage_stable_num_snapshots = 0;
    Float64 storage_stable_oldest_snapshot_lifetime = 0.0;
    UInt64 storage_stable_oldest_snapshot_thread_id = 0;
    String storage_stable_oldest_snapshot_tracing_id;

    UInt64 storage_delta_num_snapshots = 0;
    Float64 storage_delta_oldest_snapshot_lifetime = 0.0;
    UInt64 storage_delta_oldest_snapshot_thread_id = 0;
    String storage_delta_oldest_snapshot_tracing_id;

    UInt64 storage_meta_num_snapshots = 0;
    Float64 storage_meta_oldest_snapshot_lifetime = 0.0;
    UInt64 storage_meta_oldest_snapshot_thread_id = 0;
    String storage_meta_oldest_snapshot_tracing_id;

    UInt64 background_tasks_length = 0;
};

class DeltaMergeStore : private boost::noncopyable
{
public:
    friend class ::DB::DM::tests::DeltaMergeStoreTest;
    struct Settings
    {
        NotCompress not_compress_columns{};
    };
    static Settings EMPTY_SETTINGS;

    using SegmentSortedMap = std::map<RowKeyValueRef, SegmentPtr, std::less<>>;
    using SegmentMap = std::unordered_map<PageIdU64, SegmentPtr>;

    enum ThreadType
    {
        Init,
        Write,
        Read,
        BG_Split,
        BG_Merge,
        BG_MergeDelta,
        BG_Compact,
        BG_Flush,
        BG_GC,
    };

    enum TaskType
    {
        Split,
        MergeDelta,
        Compact,
        Flush,
        PlaceIndex,
    };

    struct BackgroundTask
    {
        TaskType type;

        DMContextPtr dm_context;
        SegmentPtr segment;

        explicit operator bool() const { return segment != nullptr; }
    };

    class MergeDeltaTaskPool
    {
#ifndef DBMS_PUBLIC_GTEST
    private:
#else
    public:
#endif

        using TaskQueue = std::queue<BackgroundTask, std::list<BackgroundTask>>;
        TaskQueue light_tasks;
        TaskQueue heavy_tasks;

        std::mutex mutex;

    public:
        size_t length()
        {
            std::scoped_lock lock(mutex);
            return light_tasks.size() + heavy_tasks.size();
        }

        // first element of return value means whether task is added or not
        // second element of return value means whether task is heavy or not
        std::pair<bool, bool> tryAddTask(const BackgroundTask & task, const ThreadType & whom, size_t max_task_num, const LoggerPtr & log_);

        BackgroundTask nextTask(bool is_heavy, const LoggerPtr & log_);
    };

    DeltaMergeStore(Context & db_context, //
                    bool data_path_contains_database_name,
                    const String & db_name,
                    const String & table_name_,
                    KeyspaceID keyspace_id_,
                    TableID physical_table_id_,
                    bool has_replica,
                    const ColumnDefines & columns,
                    const ColumnDefine & handle,
                    bool is_common_handle_,
                    size_t rowkey_column_size_,
                    const Settings & settings_ = EMPTY_SETTINGS,
                    ThreadPool * thread_pool = nullptr);
    ~DeltaMergeStore();

    void setUpBackgroundTask(const DMContextPtr & dm_context);

    const String & getDatabaseName() const
    {
        return db_name;
    }
    const String & getTableName() const
    {
        return table_name;
    }

    void rename(String new_path, String new_database_name, String new_table_name);

    void clearData();

    void drop();

    // Stop all background tasks.
    void shutdown();

    static Block addExtraColumnIfNeed(const Context & db_context, const ColumnDefine & handle_define, Block && block);

    void write(const Context & db_context, const DB::Settings & db_settings, Block & block);

    void deleteRange(const Context & db_context, const DB::Settings & db_settings, const RowKeyRange & delete_range);

    std::tuple<String, PageIdU64> preAllocateIngestFile();

    void preIngestFile(const String & parent_path, PageIdU64 file_id, size_t file_size);
    void removePreIngestFile(PageIdU64 file_id, bool throw_on_not_exist);

    /// You must ensure external files are ordered and do not overlap. Otherwise exceptions will be thrown.
    /// You must ensure all of the external files are contained by the range. Otherwise exceptions will be thrown.
    /// Return the 'ingested bytes'.
    UInt64 ingestFiles(const DMContextPtr & dm_context, //
                       const RowKeyRange & range,
                       const std::vector<DM::ExternalDTFileInfo> & external_files,
                       bool clear_data_in_range);

    /// You must ensure external files are ordered and do not overlap. Otherwise exceptions will be thrown.
    /// You must ensure all of the external files are contained by the range. Otherwise exceptions will be thrown.
    /// Return the 'ingtested bytes'.
    UInt64 ingestFiles(const Context & db_context, //
                       const DB::Settings & db_settings,
                       const RowKeyRange & range,
                       const std::vector<DM::ExternalDTFileInfo> & external_files,
                       bool clear_data_in_range)
    {
        auto dm_context = newDMContext(db_context, db_settings);
        return ingestFiles(dm_context, range, external_files, clear_data_in_range);
    }

    std::vector<SegmentPtr> ingestSegmentsUsingSplit(
        const DMContextPtr & dm_context,
        const RowKeyRange & ingest_range,
        const std::vector<SegmentPtr> & target_segments);

    bool ingestSegmentDataIntoSegmentUsingSplit(
        DMContext & dm_context,
        const SegmentPtr & segment,
        const RowKeyRange & ingest_range,
        const SegmentPtr & segment_to_ingest);

    void ingestSegmentsFromCheckpointInfo(const DMContextPtr & dm_context,
                                          const DM::RowKeyRange & range,
                                          CheckpointInfoPtr checkpoint_info);

    void ingestSegmentsFromCheckpointInfo(const Context & db_context,
                                          const DB::Settings & db_settings,
                                          const DM::RowKeyRange & range,
                                          CheckpointInfoPtr checkpoint_info)
    {
        auto dm_context = newDMContext(db_context, db_settings);
        return ingestSegmentsFromCheckpointInfo(dm_context, range, checkpoint_info);
    }

    /// Read all rows without MVCC filtering
    BlockInputStreams readRaw(const Context & db_context,
                              const DB::Settings & db_settings,
                              const ColumnDefines & columns_to_read,
                              size_t num_streams,
                              bool keep_order,
                              const SegmentIdSet & read_segments = {},
                              size_t extra_table_id_index = InvalidColumnID);


    /// Read rows in two modes:
    ///     when is_fast_scan == false, we will read rows with MVCC filtering, del mark !=0  filter and sorted merge.
    ///     when is_fast_scan == true, we will read rows without MVCC and sorted merge.
    /// `sorted_ranges` should be already sorted and merged.
    BlockInputStreams read(const Context & db_context,
                           const DB::Settings & db_settings,
                           const ColumnDefines & columns_to_read,
                           const RowKeyRanges & sorted_ranges,
                           size_t num_streams,
                           UInt64 max_version,
                           const PushDownFilterPtr & filter,
                           const RuntimeFilteList & runtime_filter_list,
                           const int rf_max_wait_time_ms,
                           const String & tracing_id,
                           bool keep_order,
                           bool is_fast_scan = false,
                           size_t expected_block_size = DEFAULT_BLOCK_SIZE,
                           const SegmentIdSet & read_segments = {},
                           size_t extra_table_id_index = InvalidColumnID,
                           ScanContextPtr scan_context = nullptr);


    /// Read rows in two modes:
    ///     when is_fast_scan == false, we will read rows with MVCC filtering, del mark !=0  filter and sorted merge.
    ///     when is_fast_scan == true, we will read rows without MVCC and sorted merge.
    /// `sorted_ranges` should be already sorted and merged.
    void read(
        PipelineExecutorStatus & exec_status_,
        PipelineExecGroupBuilder & group_builder,
        const Context & db_context,
        const DB::Settings & db_settings,
        const ColumnDefines & columns_to_read,
        const RowKeyRanges & sorted_ranges,
        size_t num_streams,
        UInt64 max_version,
        const PushDownFilterPtr & filter,
        const String & tracing_id,
        bool keep_order,
        bool is_fast_scan = false,
        size_t expected_block_size = DEFAULT_BLOCK_SIZE,
        const SegmentIdSet & read_segments = {},
        size_t extra_table_id_index = InvalidColumnID,
        ScanContextPtr scan_context = nullptr);

    Remote::DisaggPhysicalTableReadSnapshotPtr
    writeNodeBuildRemoteReadSnapshot(
        const Context & db_context,
        const DB::Settings & db_settings,
        const RowKeyRanges & sorted_ranges,
        size_t num_streams,
        const String & tracing_id,
        const SegmentIdSet & read_segments = {},
        ScanContextPtr scan_context = nullptr);

    /// Try flush all data in `range` to disk and return whether the task succeed.
    bool flushCache(const Context & context, const RowKeyRange & range, bool try_until_succeed = true);

    bool flushCache(const DMContextPtr & dm_context, const RowKeyRange & range, bool try_until_succeed = true);

    /// Merge delta into the stable layer for all segments.
    ///
    /// This function is called when using `MANAGE TABLE [TABLE] MERGE DELTA` from TiFlash Client.
    bool mergeDeltaAll(const Context & context);

    /// Merge delta into the stable layer for one segment located by the specified start key.
    /// Returns the range of the merged segment, which can be used to merge the remaining segments incrementally (new_start_key = old_end_key).
    /// If there is no segment found by the start key, nullopt is returned.
    ///
    /// This function is called when using `ALTER TABLE [TABLE] COMPACT ...` from TiDB.
    std::optional<DM::RowKeyRange> mergeDeltaBySegment(const Context & context, const DM::RowKeyValue & start_key);

    /// Compact the delta layer, merging multiple fragmented delta files into larger ones.
    /// This is a minor compaction as it does not merge the delta into stable layer.
    void compact(const Context & context, const RowKeyRange & range);

    /// Iterator over all segments and apply gc jobs.
    UInt64 onSyncGc(Int64 limit, const GCOptions & gc_options);

    /**
     * Try to merge the segment in the current thread as the GC operation.
     * This function may be blocking, and should be called in the GC background thread.
     */
    SegmentPtr gcTrySegmentMerge(const DMContextPtr & dm_context, const SegmentPtr & segment);

    /**
     * Try to merge delta in the current thread as the GC operation.
     * This function may be blocking, and should be called in the GC background thread.
     */
    SegmentPtr gcTrySegmentMergeDelta(const DMContextPtr & dm_context, const SegmentPtr & segment, const SegmentPtr & prev_segment, const SegmentPtr & next_segment, DB::Timestamp gc_safe_point);

    /**
     * Starting from the given base segment, find continuous segments that could be merged.
     *
     * When there are mergeable segments, the baseSegment is returned in index 0 and mergeable segments are then placed in order.
     *   It is ensured that there are at least 2 elements in the returned vector.
     * When there is no mergeable segment, the returned vector will be empty.
     */
    std::vector<SegmentPtr> getMergeableSegments(const DMContextPtr & context, const SegmentPtr & baseSegment);

    /// Apply DDL `commands` on `table_columns`
    void applyAlters(const AlterCommands & commands, //
                     OptionTableInfoConstRef table_info,
                     ColumnID & max_column_id_used,
                     const Context & context);

    ColumnDefinesPtr getStoreColumns() const
    {
        std::shared_lock lock(read_write_mutex);
        return store_columns;
    }
    const ColumnDefines & getTableColumns() const
    {
        return original_table_columns;
    }
    const ColumnDefine & getHandle() const
    {
        return original_table_handle_define;
    }
    BlockPtr getHeader() const;
    const Settings & getSettings() const
    {
        return settings;
    }
    DataTypePtr getPKDataType() const
    {
        return original_table_handle_define.type;
    }
    SortDescription getPrimarySortDescription() const;

    void check(const Context & db_context);

    StoreStats getStoreStats();
    SegmentsStats getSegmentsStats();

    bool isCommonHandle() const
    {
        return is_common_handle;
    }
    size_t getRowKeyColumnSize() const
    {
        return rowkey_column_size;
    }

    static ReadMode getReadMode(const Context & db_context, bool is_fast_scan, bool keep_order, const PushDownFilterPtr & filter);

public:
    /// Methods mainly used by region split.

    RowsAndBytes getRowsAndBytesInRange(const Context & db_context, const RowKeyRange & check_range, bool is_exact);
    RowsAndBytes getRowsAndBytesInRange(DMContext & dm_context, const RowKeyRange & check_range, bool is_exact);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    DMContextPtr newDMContext(const Context & db_context, const DB::Settings & db_settings, const String & tracing_id = "", ScanContextPtr scan_context = nullptr);

    static bool pkIsHandle(const ColumnDefine & handle_define)
    {
        return handle_define.id != EXTRA_HANDLE_COLUMN_ID;
    }

    /// Try to stall the writing. It will suspend the current thread if flow control is necessary.
    /// There are roughly two flow control mechanisms:
    /// - Force Merge (1 GB by default, see force_merge_delta_rows|size): Wait for a small amount of time at most.
    /// - Stop Write (2 GB by default, see stop_write_delta_rows|size): Wait until delta is merged.
    void waitForWrite(const DMContextPtr & context, const SegmentPtr & segment);

    void waitForDeleteRange(const DMContextPtr & context, const SegmentPtr & segment);

    /**
     * Try to update the segment. "Update" means splitting the segment into two, merging two segments, merging the delta, etc.
     * If an update is really performed, the segment will be abandoned (with `segment->hasAbandoned() == true`).
     * See `segmentSplit`, `segmentMerge`, `segmentMergeDelta` for details.
     *
     * This may be called from multiple threads, e.g. at the foreground write moment, or in background threads.
     * A `thread_type` should be specified indicating the type of the thread calling this function.
     * Depend on the thread type, the "update" to do may be varied.
     */
    void checkSegmentUpdate(const DMContextPtr & context, const SegmentPtr & segment, ThreadType thread_type);

    enum class SegmentSplitReason
    {
        ForegroundWrite,
        Background,
        ForIngest,
    };

    /**
     * Note: This enum simply shadows Segment::SplitMode without introducing the whole Segment into this header.
     */
    enum class SegmentSplitMode
    {
        /**
         * Split according to settings.
         *
         * If logical split is allowed in the settings, logical split will be tried first.
         * Logical split may fall back to physical split when calculating split point failed.
         */
        Auto,

        /**
         * Do logical split. If split point is not specified and cannot be calculated out,
         * the split will fail.
         */
        Logical,

        /**
         * Do physical split.
         */
        Physical,
    };

    /**
     * Split the segment into two.
     * After splitting, the segment will be abandoned (with `segment->hasAbandoned() == true`) and the new two segments will be returned.
     *
     * When `opt_split_at` is not specified, this function will try to find a mid point for splitting, and may lead to failures.
     */
    SegmentPair segmentSplit(DMContext & dm_context, const SegmentPtr & segment, SegmentSplitReason reason, std::optional<RowKeyValue> opt_split_at = std::nullopt, SegmentSplitMode opt_split_mode = SegmentSplitMode::Auto);

    enum class SegmentMergeReason
    {
        BackgroundGCThread,
    };

    /**
     * Merge multiple continuous segments (order by segment start key) into one.
     * Throw exception if < 2 segments are given.
     * Fail if given segments are not continuous or not valid.
     * After merging, all specified segments will be abandoned (with `segment->hasAbandoned() == true`).
     */
    SegmentPtr segmentMerge(DMContext & dm_context, const std::vector<SegmentPtr> & ordered_segments, SegmentMergeReason reason);

    enum class MergeDeltaReason
    {
        BackgroundThreadPool,
        BackgroundGCThread,
        ForegroundWrite,
        Manual,
    };

    /**
     * Merge the delta (major compaction) in the segment.
     * After delta-merging, the segment will be abandoned (with `segment->hasAbandoned() == true`) and a new segment will be returned.
     */
    SegmentPtr segmentMergeDelta(
        DMContext & dm_context,
        const SegmentPtr & segment,
        MergeDeltaReason reason,
        SegmentSnapshotPtr segment_snap = nullptr);

    /**
     * Ingest a DMFile into the segment, optionally causing a new segment being created.
     *
     * Note 1: You must ensure the DMFile is not shared in multiple segments.
     * Note 2: You must enable the GC for the DMFile by yourself.
     * Note 3: You must ensure the DMFile has been managed by the storage pool, and has been written
     *         to the PageStorage's data.

     * @param clear_all_data_in_segment Whether all data in the segment should be discarded.
     * @returns one of:
     *          - A new segment: A new segment is created for containing the data
     *          - The same segment as passed in: Data is ingested into the delta layer of current segment
     *          - nullptr: when there are errors
     */
    SegmentPtr segmentIngestData(
        DMContext & dm_context,
        const SegmentPtr & segment,
        const DMFilePtr & data_file,
        bool clear_all_data_in_segment);

    /**
     * Discard all data in the segment, and use the specified DMFile as the stable instead.
     * The specified DMFile is safe to be shared for multiple segments.
     *
     * Note 1: This function will not enable GC for the new_stable_file for you, in case of you may want to share the same
     *         stable file for multiple segments. It is your own duty to enable GC later.
     *
     * Note 2: You must ensure the specified new_stable_file has been managed by the storage pool, and has been written
     *         to the PageStorage's data. Otherwise there will be exceptions.
     *
     * Note 3: This API is subjected to be changed in future, as it relies on the knowledge that all current data
     *         in this segment is useless, which is a pretty tough requirement.
     * TODO: use `segmentIngestData` to replace this api
     */
    SegmentPtr segmentDangerouslyReplaceDataFromCheckpoint(
        DMContext & dm_context,
        const SegmentPtr & segment,
        const DMFilePtr & data_file,
        const ColumnFilePersisteds & column_file_persisteds);

    // isSegmentValid should be protected by lock on `read_write_mutex`
    bool isSegmentValid(const std::shared_lock<std::shared_mutex> &, const SegmentPtr & segment)
    {
        return doIsSegmentValid(segment);
    }
    bool isSegmentValid(const std::unique_lock<std::shared_mutex> &, const SegmentPtr & segment)
    {
        return doIsSegmentValid(segment);
    }
    bool doIsSegmentValid(const SegmentPtr & segment);

    /**
     * Ingest DTFiles directly into the stable layer by splitting segments.
     * This strategy can be used only when the destination range is cleared before ingesting.
     */
    std::vector<SegmentPtr> ingestDTFilesUsingSplit(
        const DMContextPtr & dm_context,
        const RowKeyRange & range,
        const std::vector<ExternalDTFileInfo> & external_files,
        const std::vector<DMFilePtr> & files,
        bool clear_data_in_range);

    std::vector<SegmentPtr> ingestDTFilesUsingColumnFile(
        const DMContextPtr & dm_context,
        const RowKeyRange & range,
        const std::vector<DMFilePtr> & files,
        bool clear_data_in_range);

    bool ingestDTFileIntoSegmentUsingSplit(
        DMContext & dm_context,
        const SegmentPtr & segment,
        const RowKeyRange & ingest_range,
        const DMFilePtr & file,
        bool clear_data_in_range);

    bool updateGCSafePoint();

    bool handleBackgroundTask(bool heavy);

    void listLocalStableFiles(const std::function<void(UInt64, const String &)> & handle) const;
    void restoreStableFiles() const;
    void restoreStableFilesFromLocal() const;
    void removeLocalStableFilesIfDisagg() const;

    SegmentReadTasks getReadTasksByRanges(DMContext & dm_context,
                                          const RowKeyRanges & sorted_ranges,
                                          size_t expected_tasks_count = 1,
                                          const SegmentIdSet & read_segments = {},
                                          bool try_split_task = true);

private:
    void dropAllSegments(bool keep_first_segment);
    String getLogTracingId(const DMContext & dm_ctx);

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif

    Context & global_context;
    std::shared_ptr<StoragePathPool> path_pool;
    Settings settings;
    StoragePoolPtr storage_pool;

    String db_name;
    String table_name;

    const KeyspaceID keyspace_id;
    const TableID physical_table_id;

    const bool is_common_handle;
    const size_t rowkey_column_size;

    ColumnDefines original_table_columns;
    BlockPtr original_table_header; // Used to speed up getHeader()
    ColumnDefine original_table_handle_define;

    // The columns we actually store.
    // First three columns are always _tidb_rowid, _INTERNAL_VERSION, _INTERNAL_DELMARK
    // No matter `tidb_rowid` exist in `table_columns` or not.
    ColumnDefinesPtr store_columns;

    std::atomic<bool> shutdown_called{false};
    std::atomic<bool> replica_exist{true};

    BackgroundProcessingPool & background_pool;
    BackgroundProcessingPool::TaskHandle background_task_handle;

    BackgroundProcessingPool & blockable_background_pool;
    BackgroundProcessingPool::TaskHandle blockable_background_pool_handle;

    /// end of range -> segment
    SegmentSortedMap segments;
    /// Mainly for debug.
    SegmentMap id_to_segment;

    MergeDeltaTaskPool background_tasks;

    std::atomic<DB::Timestamp> latest_gc_safe_point = 0;

    RowKeyValue next_gc_check_key;

    // Synchronize between write threads and read threads.
    mutable std::shared_mutex read_write_mutex;

    LoggerPtr log;
}; // namespace DM

using DeltaMergeStorePtr = std::shared_ptr<DeltaMergeStore>;

} // namespace DM
} // namespace DB

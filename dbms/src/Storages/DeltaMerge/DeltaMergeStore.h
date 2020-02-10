#pragma once

#include <queue>

#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Storages/AlterCommands.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

namespace DM
{

static const PageId DELTA_MERGE_FIRST_SEGMENT_ID = 1;

struct DeltaMergeStoreStat
{
    UInt64  segment_count    = 0;
    Float64 delta_rate_rows  = 0;
    Float64 delta_rate_count = 0;

    UInt64 total_rows          = 0;
    UInt64 total_bytes         = 0;
    UInt64 total_delete_ranges = 0;

    Float64 delta_placed_rate = 0;

    Float64 avg_segment_rows  = 0;
    Float64 avg_segment_bytes = 0;

    UInt64  delta_count             = 0;
    UInt64  total_delta_rows        = 0;
    UInt64  total_delta_bytes       = 0;
    Float64 avg_delta_rows          = 0;
    Float64 avg_delta_bytes         = 0;
    Float64 avg_delta_delete_ranges = 0;

    UInt64  stable_count       = 0;
    UInt64  total_stable_rows  = 0;
    UInt64  total_stable_bytes = 0;
    Float64 avg_stable_rows    = 0;
    Float64 avg_stable_bytes   = 0;

    UInt64  total_pack_count_in_delta = 0;
    Float64 avg_pack_count_in_delta   = 0;
    Float64 avg_pack_rows_in_delta    = 0;
    Float64 avg_pack_bytes_in_delta   = 0;

    UInt64  total_pack_count_in_stable = 0;
    Float64 avg_pack_count_in_stable   = 0;
    Float64 avg_pack_rows_in_stable    = 0;
    Float64 avg_pack_bytes_in_stable   = 0;

    UInt64 storage_stable_num_snapshots    = 0;
    UInt64 storage_stable_num_pages        = 0;
    UInt64 storage_stable_num_normal_pages = 0;
    UInt64 storage_stable_max_page_id      = 0;

    UInt64 storage_delta_num_snapshots    = 0;
    UInt64 storage_delta_num_pages        = 0;
    UInt64 storage_delta_num_normal_pages = 0;
    UInt64 storage_delta_max_page_id      = 0;

    UInt64 storage_meta_num_snapshots    = 0;
    UInt64 storage_meta_num_pages        = 0;
    UInt64 storage_meta_num_normal_pages = 0;
    UInt64 storage_meta_max_page_id      = 0;
};

// It is used to prevent hash conflict of file caches.
static std::atomic<UInt64> DELTA_MERGE_STORE_HASH_SALT{0};

class DeltaMergeStore : private boost::noncopyable
{
    using OpContext = DiskValueSpace::OpContext;

public:
    struct Settings
    {
        NotCompress not_compress_columns{};
    };

    struct WriteAction
    {
        SegmentPtr    segment;
        const size_t  offset = 0;
        const size_t  limit  = 0;
        BlockOrDelete update = {};

        AppendTaskPtr task = {};

        WriteAction(){};
        WriteAction(const SegmentPtr & segment_, size_t offset_, size_t limit_) : segment(segment_), offset(offset_), limit(limit_) {}
        WriteAction(const SegmentPtr & segment_, const BlockOrDelete & update_) : segment(segment_), update(update_) {}
    };

    using WriteActions     = std::vector<WriteAction>;
    using SegmentSortedMap = std::map<Handle, SegmentPtr>;

    enum BackgroundType
    {
        MergeDelta,
        SegmentMerge
    };

    static std::string getBackgroundTypeName(BackgroundType type)
    {
        switch (type)
        {
        case BackgroundType ::MergeDelta:
            return "MergeDelta";
        case BackgroundType ::SegmentMerge:
            return "SegmentMerge";
        default:
            return "Unknown";
        }
    }

    struct BackgroundTask
    {
        DMContextPtr   dm_context = {};
        SegmentPtr     segment    = {};
        BackgroundType type;

        explicit operator bool() { return (bool)segment; }
    };

    class MergeDeltaTaskPool
    {
    private:
        using TaskQueue = std::queue<BackgroundTask, std::list<BackgroundTask>>;
        TaskQueue tasks;

        std::mutex mutex;

    public:
        void addTask(const BackgroundTask & task, const String & whom, Logger * log_)
        {
            LOG_DEBUG(log_,
                      "Segment [" << task.segment->segmentId() << "] task [" << getBackgroundTypeName(task.type)
                                  << "] add to background task pool by [" << whom << "]");

            std::scoped_lock lock(mutex);
            tasks.push(task);
        }

        BackgroundTask nextTask(Logger * log_)
        {
            std::scoped_lock lock(mutex);

            if (tasks.empty())
                return {};
            auto task = tasks.front();
            tasks.pop();

            LOG_DEBUG(log_,
                      "Segment [" << task.segment->segmentId() << "] task [" << getBackgroundTypeName(task.type)
                                  << "] pop from background task pool");

            return task;
        }
    };

    DeltaMergeStore(Context &             db_context, //
                    const String &        path_,
                    const String &        db_name,
                    const String &        tbl_name,
                    const ColumnDefines & columns,
                    const ColumnDefine &  handle,
                    const Settings &      settings_);
    ~DeltaMergeStore();

    void write(const Context & db_context, const DB::Settings & db_settings, const Block & block);

    void deleteRange(const Context & db_context, const DB::Settings & db_settings, const HandleRange & delete_range);

    BlockInputStreams
    readRaw(const Context & db_context, const DB::Settings & db_settings, const ColumnDefines & column_defines, size_t num_streams);

    /// ranges should be sorted and merged already.
    BlockInputStreams read(const Context &       db_context,
                           const DB::Settings &  db_settings,
                           const ColumnDefines & columns_to_read,
                           const HandleRanges &  sorted_ranges,
                           size_t                num_streams,
                           UInt64                max_version,
                           const RSOperatorPtr & filter,
                           size_t                expected_block_size = DEFAULT_BLOCK_SIZE);

    /// Force flush all data to disk.
    /// Now is called by `StorageDeltaMerge`'s `alter` / `rename`
    /// and no other threads is able to read / write at the same time.
    void flushCache(const Context & context);

    /// Apply `commands` on `table_columns`
    void applyAlters(const AlterCommands &         commands, //
                     const OptionTableInfoConstRef table_info,
                     ColumnID &                    max_column_id_used,
                     const Context &               context);

    const ColumnDefines & getTableColumns() const { return table_columns; }
    const ColumnDefine &  getHandle() const { return table_handle_define; }
    Block                 getHeader() const { return toEmptyBlock(table_columns); }
    const Settings &      getSettings() const { return settings; }
    DataTypePtr           getPKDataType() const { return table_handle_define.type; }
    SortDescription       getPrimarySortDescription() const;

    void                check(const Context & db_context);
    DeltaMergeStoreStat getStat();

private:
    DMContextPtr newDMContext(const Context & db_context, const DB::Settings & db_settings);

    bool pkIsHandle() const { return table_handle_define.id != EXTRA_HANDLE_COLUMN_ID; }

    template <bool by_write_thread>
    void checkSegmentUpdate(const DMContextPtr & context, SegmentPtr segment);

    SegmentPair segmentSplit(DMContext & dm_context, const SegmentPtr & segment);
    void        segmentMerge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right);
    SegmentPtr  segmentMergeDelta(DMContext &             dm_context,
                                  const SegmentPtr &      segment,
                                  const SegmentSnapshot & segment_snap,
                                  const StorageSnapshot & storage_snap,
                                  bool                    is_foreground);

    SegmentPtr segmentForegroundMergeDelta(DMContext & dm_context, const SegmentPtr & segment);
    void       segmentBackgroundMergeDelta(DMContext & dm_context, const SegmentPtr & segment);
    void       segmentForegroundMerge(DMContext & dm_context, const SegmentPtr & segment);

    bool handleBackgroundTask();

    void applyAlter(const AlterCommand &          command, //
                    const OptionTableInfoConstRef table_info,
                    ColumnID &                    max_column_id_used);

    void commitWrites(WriteActions && actions, WriteBatches & wbs, const DMContextPtr & dm_context, OpContext & op_context);

    bool isSegmentValid(const SegmentPtr & segment);

private:
    String      path;
    PathPool    extra_paths;
    StoragePool storage_pool;

    String        db_name;
    String        table_name;
    ColumnDefines table_columns;
    ColumnDefine  table_handle_define;

    BackgroundProcessingPool &           background_pool;
    BackgroundProcessingPool::TaskHandle gc_handle;
    BackgroundProcessingPool::TaskHandle background_task_handle;

    Context & global_context;
    Settings  settings;

    /// end of range -> segment
    SegmentSortedMap segments;

    MergeDeltaTaskPool merge_delta_tasks;

    // Synchronize between one write thread and multiple read threads.
    std::shared_mutex read_write_mutex;

    // Used to guarantee only one thread can do write.
    std::mutex write_write_mutex;

    UInt64   hash_salt;
    Logger * log;
}; // namespace DM

using DeltaMergeStorePtr = std::shared_ptr<DeltaMergeStore>;

} // namespace DM
} // namespace DB

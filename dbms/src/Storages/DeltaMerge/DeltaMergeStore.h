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
#include <Storages/Transaction/TiDB.h>

namespace DB
{

namespace DM
{

static constexpr size_t DELTA_MERGE_DEFAULT_SEGMENT_ROWS = DEFAULT_BLOCK_SIZE << 6;
static const PageId     DELTA_MERGE_FIRST_SEGMENT_ID     = 1;

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
        const SegmentPtr segment;
        const size_t     offset = 0;
        const size_t     limit  = 0;
        BlockOrDelete    update = {};

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
                                  << "] add to background task pool by" << whom);

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

    void setMinDataVersion(UInt64 version) { min_version = version; }

    const ColumnDefines & getTableColumns() const { return table_columns; }
    const ColumnDefine &  getHandle() const { return table_handle_define; }
    Block                 getHeader() const { return toEmptyBlock(table_columns); }
    const Settings &      getSettings() const { return settings; }
    DataTypePtr           getPKDataType() const { return table_handle_define.type; }
    SortDescription       getPrimarySortDescription() const;

    void check(const Context & db_context);

private:
    DMContextPtr newDMContext(const Context & db_context, const DB::Settings & db_settings)
    {
        ColumnDefines store_columns = table_columns;
        if (pkIsHandle())
        {
            // Add an extra handle column.
            store_columns.push_back(getExtraHandleColumnDefine());
        }

        auto * ctx = new DMContext{.db_context    = db_context,
                                   .storage_pool  = storage_pool,
                                   .store_columns = std::move(store_columns),
                                   .handle_column = getExtraHandleColumnDefine(),
                                   .min_version   = min_version,

                                   .not_compress            = settings.not_compress_columns,
                                   .segment_limit_rows      = db_settings.dm_segment_limit_rows,
                                   .delta_limit_rows        = db_settings.dm_segment_delta_limit_rows,
                                   .delta_limit_bytes       = db_settings.dm_segment_delta_limit_bytes,
                                   .delta_cache_limit_rows  = db_settings.dm_segment_delta_cache_limit_rows,
                                   .delta_cache_limit_bytes = db_settings.dm_segment_delta_cache_limit_bytes};
        return DMContextPtr(ctx);
    }

    bool pkIsHandle() const { return table_handle_define.id != EXTRA_HANDLE_COLUMN_ID; }

    template <bool by_write_thread>
    void checkSegmentUpdate(const DMContextPtr & context, const SegmentPtr & segment);

    SegmentPair segmentSplit(DMContext & dm_context, const SegmentPtr & segment);
    void        segmentMerge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right);
    void        segmentMergeDelta(DMContext &             dm_context,
                                  const SegmentPtr &      segment,
                                  const SegmentSnapshot & segment_snap,
                                  const StorageSnapshot & storage_snap,
                                  bool                    is_foreground);

    void segmentForegroundMergeDelta(DMContext & dm_context, const SegmentPtr & segment);
    void segmentBackgroundMergeDelta(DMContext & dm_context, const SegmentPtr & segment);
    void segmentForegroundMerge(DMContext & dm_context, const SegmentPtr & segment);

    bool handleBackgroundTask();

    void applyAlter(const AlterCommand &          command, //
                    const OptionTableInfoConstRef table_info,
                    ColumnID &                    max_column_id_used);

    void commitWrites(const WriteActions & actions, WriteBatches & wbs, const DMContextPtr & dm_context, OpContext & op_context);

    bool isSegmentValid(const SegmentPtr & segment);

private:
    String      path;
    StoragePool storage_pool;

    String        table_name;
    ColumnDefines table_columns;
    ColumnDefine  table_handle_define;

    BackgroundProcessingPool &           background_pool;
    BackgroundProcessingPool::TaskHandle gc_handle;
    BackgroundProcessingPool::TaskHandle background_task_handle;

    Settings settings;

    UInt64 min_version = 0;

    /// end of range -> segment
    SegmentSortedMap segments;

    MergeDeltaTaskPool merge_delta_tasks;

    // Synchronize between one write thread and multiple read threads.
    std::shared_mutex read_write_mutex;

    // Used to guarantee only one thread can do write.
    std::mutex write_write_mutex;

    Logger * log;
}; // namespace DM

using DeltaMergeStorePtr = std::shared_ptr<DeltaMergeStore>;

} // namespace DM
} // namespace DB
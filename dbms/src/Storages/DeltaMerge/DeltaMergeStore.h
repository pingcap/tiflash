#pragma once

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/AlterCommands.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/TiDB.h>

#include <queue>

namespace DB
{

namespace DM
{
class Segment;
using SegmentPtr  = std::shared_ptr<Segment>;
using SegmentPair = std::pair<SegmentPtr, SegmentPtr>;
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;
struct DMContext;
using DMContextPtr = std::shared_ptr<DMContext>;
using NotCompress  = std::unordered_set<ColId>;
using SegmentIdSet = std::unordered_set<UInt64>;

static const PageId DELTA_MERGE_FIRST_SEGMENT_ID = 1;

struct SegmentStat
{
    UInt64      segment_id;
    RowKeyRange range;

    UInt64 rows          = 0;
    UInt64 size          = 0;
    UInt64 delete_ranges = 0;

    UInt64 stable_size_on_disk = 0;

    UInt64 delta_pack_count  = 0;
    UInt64 stable_pack_count = 0;

    Float64 avg_delta_pack_rows  = 0;
    Float64 avg_stable_pack_rows = 0;

    Float64 delta_rate       = 0;
    UInt64  delta_cache_size = 0;

    UInt64 delta_index_size = 0;
};
using SegmentStats = std::vector<SegmentStat>;

struct DeltaMergeStoreStat
{
    UInt64 segment_count = 0;

    UInt64 total_rows          = 0;
    UInt64 total_size          = 0;
    UInt64 total_delete_ranges = 0;

    Float64 delta_rate_rows     = 0;
    Float64 delta_rate_segments = 0;

    Float64 delta_placed_rate       = 0;
    UInt64  delta_cache_size        = 0;
    Float64 delta_cache_rate        = 0;
    Float64 delta_cache_wasted_rate = 0;

    UInt64 delta_index_size = 0;

    Float64 avg_segment_rows = 0;
    Float64 avg_segment_size = 0;

    UInt64  delta_count             = 0;
    UInt64  total_delta_rows        = 0;
    UInt64  total_delta_size        = 0;
    Float64 avg_delta_rows          = 0;
    Float64 avg_delta_size          = 0;
    Float64 avg_delta_delete_ranges = 0;

    UInt64  stable_count              = 0;
    UInt64  total_stable_rows         = 0;
    UInt64  total_stable_size         = 0;
    UInt64  total_stable_size_on_disk = 0;
    Float64 avg_stable_rows           = 0;
    Float64 avg_stable_size           = 0;

    UInt64  total_pack_count_in_delta = 0;
    Float64 avg_pack_count_in_delta   = 0;
    Float64 avg_pack_rows_in_delta    = 0;
    Float64 avg_pack_size_in_delta    = 0;

    UInt64  total_pack_count_in_stable = 0;
    Float64 avg_pack_count_in_stable   = 0;
    Float64 avg_pack_rows_in_stable    = 0;
    Float64 avg_pack_size_in_stable    = 0;

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

    UInt64 background_tasks_length = 0;
};

// It is used to prevent hash conflict of file caches.
static std::atomic<UInt64> DELTA_MERGE_STORE_HASH_SALT{0};

class DeltaMergeStore : private boost::noncopyable
{
public:
    struct Settings
    {
        NotCompress not_compress_columns{};
    };

    using SegmentSortedMap = std::map<RowKeyValueRef, SegmentPtr, std::less<>>;
    using SegmentMap       = std::unordered_map<PageId, SegmentPtr>;

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
    };

    enum TaskType
    {
        Split,
        Merge,
        MergeDelta,
        Compact,
        Flush,
        PlaceIndex,
    };

    static std::string toString(ThreadType type)
    {
        switch (type)
        {
        case Init:
            return "Init";
        case Write:
            return "Write";
        case Read:
            return "Read";
        case BG_Split:
            return "BG_Split";
        case BG_Merge:
            return "BG_Merge";
        case BG_MergeDelta:
            return "BG_MergeDelta";
        case BG_Compact:
            return "BG_Compact";
        case BG_Flush:
            return "BG_Flush";
        default:
            return "Unknown";
        }
    }

    static std::string toString(TaskType type)
    {
        switch (type)
        {
        case Split:
            return "Split";
        case Merge:
            return "Merge";
        case MergeDelta:
            return "MergeDelta";
        case Compact:
            return "Compact";
        case Flush:
            return "Flush";
        case PlaceIndex:
            return "PlaceIndex";
        default:
            return "Unknown";
        }
    }

    struct BackgroundTask
    {
        TaskType type;

        DMContextPtr dm_context;
        SegmentPtr   segment;
        SegmentPtr   next_segment;

        explicit operator bool() { return (bool)segment; }
    };

    class MergeDeltaTaskPool
    {
    private:
        using TaskQueue = std::queue<BackgroundTask, std::list<BackgroundTask>>;
        TaskQueue tasks;

        std::mutex mutex;

    public:
        size_t length() { return tasks.size(); }

        void addTask(const BackgroundTask & task, const ThreadType & whom, Logger * log_);

        BackgroundTask nextTask(Logger * log_);
    };

    DeltaMergeStore(Context &             db_context, //
                    const String &        path_,
                    bool                  data_path_contains_database_name,
                    const String &        db_name,
                    const String &        tbl_name,
                    const ColumnDefines & columns,
                    const ColumnDefine &  handle,
                    bool                  is_common_handle_,
                    size_t                rowkey_column_size_,
                    const Settings &      settings_);
    ~DeltaMergeStore();

    void setUpBackgroundTask(const DMContextPtr & dm_context);

    const String & getDatabaseName() const { return db_name; }
    const String & getTableName() const { return table_name; }

    void rename(String new_path, bool clean_rename, String new_database_name, String new_table_name);

    void drop();

    // Stop all background tasks.
    void shutdown();

    void write(const Context & db_context, const DB::Settings & db_settings, const Block & block);

    // Deprated
    void deleteRange(const Context & db_context, const DB::Settings & db_settings, const RowKeyRange & delete_range);

    BlockInputStreams readRaw(const Context &       db_context,
                              const DB::Settings &  db_settings,
                              const ColumnDefines & column_defines,
                              size_t                num_streams,
                              const SegmentIdSet &  read_segments = {});

    /// ranges should be sorted and merged already.
    BlockInputStreams read(const Context &       db_context,
                           const DB::Settings &  db_settings,
                           const ColumnDefines & columns_to_read,
                           const RowKeyRanges &  sorted_ranges,
                           size_t                num_streams,
                           UInt64                max_version,
                           const RSOperatorPtr & filter,
                           size_t                expected_block_size = DEFAULT_BLOCK_SIZE,
                           const SegmentIdSet &  read_segments       = {});

    /// Force flush all data to disk.
    void flushCache(const Context & context, const RowKeyRange & range)
    {
        auto dm_context = newDMContext(context, context.getSettingsRef());
        flushCache(dm_context, range);
    }

    void flushCache(const DMContextPtr & dm_context, const RowKeyRange & range);

    /// Do merge delta for all segments. Only used for debug.
    void mergeDeltaAll(const Context & context);

    /// Compact fregment packs into bigger one.
    void compact(const Context & context, const RowKeyRange & range);

    /// Apply `commands` on `table_columns`
    void applyAlters(const AlterCommands &         commands, //
                     const OptionTableInfoConstRef table_info,
                     ColumnID &                    max_column_id_used,
                     const Context &               context);

    const ColumnDefines & getTableColumns() const { return original_table_columns; }
    const ColumnDefine &  getHandle() const { return original_table_handle_define; }
    BlockPtr              getHeader() const;
    const Settings &      getSettings() const { return settings; }
    DataTypePtr           getPKDataType() const { return original_table_handle_define.type; }
    SortDescription       getPrimarySortDescription() const;

    void                check(const Context & db_context);
    DeltaMergeStoreStat getStat();
    SegmentStats        getSegmentStats();
    bool                isCommonHandle() const { return is_common_handle; }
    size_t              getRowKeyColumnSize() const { return rowkey_column_size; }

private:
    DMContextPtr newDMContext(const Context & db_context, const DB::Settings & db_settings);

    bool pkIsHandle() const { return original_table_handle_define.id != EXTRA_HANDLE_COLUMN_ID; }

    void waitForWrite(const DMContextPtr & context, const SegmentPtr & segment);
    void waitForDeleteRange(const DMContextPtr & context, const SegmentPtr & segment);

    void checkSegmentUpdate(const DMContextPtr & context, const SegmentPtr & segment, ThreadType thread_type);

    SegmentPair segmentSplit(DMContext & dm_context, const SegmentPtr & segment);
    void        segmentMerge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right);
    SegmentPtr  segmentMergeDelta(DMContext & dm_context, const SegmentPtr & segment, bool is_foreground);

    bool handleBackgroundTask();

    bool isSegmentValid(const SegmentPtr & segment);

    void restoreExtraPathCapacity();

private:
    String      path;
    PathPool    extra_paths;
    Context &   global_context;
    Settings    settings;
    StoragePool storage_pool;

    String db_name;
    String table_name;

    bool   is_common_handle;
    size_t rowkey_column_size;

    ColumnDefines original_table_columns;
    BlockPtr      original_table_header; // Used to speed up getHeader()
    ColumnDefine  original_table_handle_define;

    // The columns we actually store.
    // First three columns are always _tidb_rowid, _INTERNAL_VERSION, _INTERNAL_DELMARK
    // No matter `tidb_rowid` exist in `table_columns` or not.
    ColumnDefinesPtr store_columns;

    std::atomic<bool> shutdown_called{false};

    BackgroundProcessingPool &           background_pool;
    BackgroundProcessingPool::TaskHandle gc_handle;
    BackgroundProcessingPool::TaskHandle background_task_handle;

    /// end of range -> segment
    SegmentSortedMap segments;
    /// Mainly for debug.
    SegmentMap id_to_segment;

    MergeDeltaTaskPool background_tasks;

    DB::Timestamp latest_gc_safe_point = 0;

    // Synchronize between write threads and read threads.
    mutable std::shared_mutex read_write_mutex;

    UInt64   hash_salt;
    Logger * log;
}; // namespace DM

using DeltaMergeStorePtr = std::shared_ptr<DeltaMergeStore>;

} // namespace DM
} // namespace DB

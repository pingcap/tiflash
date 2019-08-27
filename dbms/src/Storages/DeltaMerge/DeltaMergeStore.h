#pragma once

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

class DeltaMergeStore
{
    using OpContext = DiskValueSpace::OpContext;

public:
    struct Settings
    {
        NotCompress not_compress_columns{};

        // TODO: Make this setting table specified.

        //        size_t segment_rows = DELTA_MERGE_DEFAULT_SEGMENT_ROWS;
        //
        //        // The threshold of delta.
        //        size_t segment_delta_limit_rows  = DELTA_MERGE_DEFAULT_SEGMENT_ROWS / 10;
        //        size_t segment_delta_limit_bytes = 64 * MB;
        //
        //        // The threshold of cache in delta.
        //        size_t segment_delta_cache_limit_rows  = DEFAULT_BLOCK_SIZE;
        //        size_t segment_delta_cache_limit_bytes = 16 * MB;
    };

    DeltaMergeStore(Context &             db_context, //
                    const String &        path_,
                    const String &        name,
                    const ColumnDefines & columns,
                    const ColumnDefine &  handle,
                    const Settings &      settings_);
    ~DeltaMergeStore();

    void write(const Context & db_context, const DB::Settings & db_settings, const Block & block);

    BlockInputStreams
    readRaw(const Context & db_context, const DB::Settings & db_settings, const ColumnDefines & column_defines, size_t num_streams);

    /// ranges should be sorted and merged already.
    BlockInputStreams read(const Context &       db_context,
                           const DB::Settings &  db_settings,
                           const ColumnDefines & columns_to_read,
                           const HandleRanges &  sorted_ranges,
                           size_t                num_streams,
                           UInt64                max_version,
                           size_t                expected_block_size);

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
    const Block &         getHeader() const { return header; }
    const Settings &      getSettings() const { return settings; }

    void check(const Context & db_context, const DB::Settings & db_settings);

private:
    DMContext newDMContext(const Context & db_context, const DB::Settings & db_settings)
    {
        return DMContext{.db_context          = db_context,
                         .storage_pool        = storage_pool,
                         .table_columns       = table_columns,
                         .table_handle_define = table_handle_define,
                         .min_version         = min_version,

                         .not_compress            = settings.not_compress_columns,
                         .delta_limit_rows        = db_settings.dm_segment_delta_limit_rows,
                         .delta_limit_bytes       = db_settings.dm_segment_delta_limit_bytes,
                         .delta_cache_limit_rows  = db_settings.dm_segment_delta_cache_limit_rows,
                         .delta_cache_limit_bytes = db_settings.dm_segment_delta_cache_limit_bytes};
    }

    bool afterInsertOrDelete(const Context & db_context, const DB::Settings & db_settings);
    bool shouldSplit(const SegmentPtr & segment, size_t segment_rows_setting);
    bool shouldMerge(const SegmentPtr & left, const SegmentPtr & right, size_t segment_rows_setting);
    void split(DMContext & dm_context, const SegmentPtr & segment);
    void merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right);

    void applyAlter(const AlterCommand &          command, //
                    const OptionTableInfoConstRef table_info,
                    ColumnID &                    max_column_id_used);

    static Block genHeaderBlock(const ColumnDefines & raw_columns, //
                                const ColumnDefine &  handle_define,
                                const DataTypePtr &   handle_real_type);

private:
    using SegmentSortedMap = std::map<Handle, SegmentPtr>;

    String      path;
    StoragePool storage_pool;

    String        table_name;
    ColumnDefines table_columns;
    ColumnDefine  table_handle_define;
    DataTypePtr   table_handle_real_type;
    Block         header; // an empty block header

    BackgroundProcessingPool &           background_pool;
    BackgroundProcessingPool::TaskHandle gc_handle;

    Settings settings;

    UInt64 min_version = 0;

    /// end of range -> segment
    SegmentSortedMap segments;

    std::shared_mutex mutex;

    Logger * log;
};

using DeltaMergeStorePtr = std::shared_ptr<DeltaMergeStore>;

} // namespace DM
} // namespace DB
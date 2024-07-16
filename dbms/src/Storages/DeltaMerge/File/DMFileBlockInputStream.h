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

#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>


namespace DB::DM
{
inline static constexpr size_t DMFILE_READ_ROWS_THRESHOLD = DEFAULT_MERGE_BLOCK_SIZE * 3;

class DMFileBlockInputStream : public SkippableBlockInputStream
{
public:
    DMFileBlockInputStream(DMFileReader && reader_, bool enable_data_sharing_, const PredicateFilterPtr & filter_)
        : reader(std::move(reader_))
        , enable_data_sharing(enable_data_sharing_)
        , extra_cast(filter_ ? filter_->extra_cast : nullptr)
        , filter(filter_ ? std::optional(filter_->getFilterTransformAction(reader.getHeader())) : std::nullopt)
        , project_after_where(filter_ ? filter_->project_after_where : nullptr)
        , filter_column_name(filter_ ? filter_->filter_column_name : "")
    {
        if (enable_data_sharing)
        {
            DMFileReaderPool::instance().add(reader);
        }
    }

    ~DMFileBlockInputStream() override
    {
        if (enable_data_sharing)
        {
            DMFileReaderPool::instance().del(reader);
        }
    }

    String getName() const override { return "DMFile"; }

    Block getHeader() const override { return reader.getHeader(); }

    bool getSkippedRows(size_t & skip_rows) override { return reader.getSkippedRows(skip_rows); }

    size_t skipNextBlock() override { return reader.skipNextBlock(); }

    Block read() override
    {
        FilterPtr filter_ignored;
        return read(filter_ignored, false);
    }

    Block readWithFilter(const IColumn::Filter & filter, FilterPtr & res_filter, bool return_filter) override;

    Block read(FilterPtr & res_filter, bool return_filter) override;

private:
    bool filterBlock(Block & block, FilterPtr & res_filter, bool return_filter);

    friend class tests::DMFileMetaV2Test;
    DMFileReader reader;
    const bool enable_data_sharing;

    ExpressionActionsPtr extra_cast;
    std::optional<FilterTransformAction> filter;
    ExpressionActionsPtr project_after_where;
    String filter_column_name;

    IColumn::Filter filter_result;
};

using DMFileBlockInputStreamPtr = std::shared_ptr<DMFileBlockInputStream>;

class DMFileBlockInputStreamBuilder
{
public:
    // Construct a builder by `context`.
    // It implicitly set the params by
    // - mark cache and min-max-index cache from global context
    // - current settings from this context
    // - current read limiter form this context
    // - current file provider from this context
    explicit DMFileBlockInputStreamBuilder(const Context & dm_context);

    // Build the final stream ptr.
    // Empty `rowkey_ranges` means not filter by rowkey
    // Should not use the builder again after `build` is called.
    DMFileBlockInputStreamPtr build(
        const DMFilePtr & dmfile,
        const ColumnDefines & read_columns,
        const RowKeyRanges & rowkey_ranges,
        const ScanContextPtr & scan_context);

    // **** filters **** //

    // Only set enable_handle_clean_read_ param to true when
    //    in normal mode (is_fast_scan_ == false):
    //      1. There is no delta.
    //      2. You don't need pk, version and delete_tag columns
    //    in fast scan mode (is_fast_scan_ == true):
    //      1. You don't need pk columns
    //    If you have no idea what it means, then simply set it to false.
    // Only set enable_del_clean_read_ param to true when you don't need del columns in fast scan.
    // `max_data_version_` is the MVCC filter version for reading. Used by clean read check
    DMFileBlockInputStreamBuilder & enableCleanRead(
        bool enable_handle_clean_read_,
        bool is_fast_scan_,
        bool enable_del_clean_read_,
        UInt64 max_data_version_)
    {
        enable_handle_clean_read = enable_handle_clean_read_;
        enable_del_clean_read = enable_del_clean_read_;
        is_fast_scan = is_fast_scan_;
        max_data_version = max_data_version_;
        return *this;
    }

    DMFileBlockInputStreamBuilder & setRSOperator(const RSOperatorPtr & filter_)
    {
        rs_filter = filter_;
        return *this;
    }

    DMFileBlockInputStreamBuilder & setReadPacks(const IdSetPtr & read_packs_)
    {
        read_packs = read_packs_;
        return *this;
    }

    DMFileBlockInputStreamBuilder & setColumnCache(const ColumnCachePtr & column_cache_)
    {
        // note that `enable_column_cache` is controlled by Settings (see `setFromSettings`)
        column_cache = column_cache_;
        return *this;
    }

    DMFileBlockInputStreamBuilder & onlyReadOnePackEveryTime()
    {
        read_one_pack_every_time = true;
        return *this;
    }
    DMFileBlockInputStreamBuilder & setRowsThreshold(size_t rows_threshold_per_read_)
    {
        rows_threshold_per_read = rows_threshold_per_read_;
        return *this;
    }

    DMFileBlockInputStreamBuilder & setTracingID(const String & tracing_id_)
    {
        tracing_id = tracing_id_;
        return *this;
    }

    DMFileBlockInputStreamBuilder & setReadTag(ReadTag read_tag_)
    {
        read_tag = read_tag_;
        return *this;
    }

    DMFileBlockInputStreamBuilder & setFilter(const PredicateFilterPtr & filter_)
    {
        filter = filter_;
        return *this;
    }

private:
    // These methods are called by the ctor

    DMFileBlockInputStreamBuilder & setFromSettings(const Settings & settings);

    DMFileBlockInputStreamBuilder & setCaches(
        const MarkCachePtr & mark_cache_,
        const MinMaxIndexCachePtr & index_cache_)
    {
        mark_cache = mark_cache_;
        index_cache = index_cache_;
        return *this;
    }

private:
    FileProviderPtr file_provider;

    // clean read

    bool enable_handle_clean_read = false;
    bool is_fast_scan = false;
    bool enable_del_clean_read = false;
    UInt64 max_data_version = std::numeric_limits<UInt64>::max();
    // Rough set filter
    RSOperatorPtr rs_filter;
    // packs filter (filter by pack index)
    IdSetPtr read_packs{};
    MarkCachePtr mark_cache;
    MinMaxIndexCachePtr index_cache;
    // column cache
    bool enable_column_cache = false;
    ColumnCachePtr column_cache;
    ReadLimiterPtr read_limiter;
    size_t max_read_buffer_size{};
    size_t rows_threshold_per_read = DMFILE_READ_ROWS_THRESHOLD;
    bool read_one_pack_every_time = false;
    size_t max_sharing_column_bytes_for_all = 0;
    String tracing_id;
    ReadTag read_tag = ReadTag::Internal;
    PredicateFilterPtr filter;
};

/**
 * Create a simple stream that read all blocks on default.
 * Only read one pack every time.
 * @param context Database context.
 * @param file DMFile pointer.
 * @param cols The columns to read. Empty means read all columns.
 * @return A shared pointer of an input stream
 */
DMFileBlockInputStreamPtr createSimpleBlockInputStream(
    const DB::Context & context,
    const DMFilePtr & file,
    ColumnDefines cols = {});

} // namespace DB::DM

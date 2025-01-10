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
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterView.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/File/ColumnCacheLongTerm_fwd.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/File/DMFileWithVectorIndexBlockInputStream_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex_fwd.h>
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
    explicit DMFileBlockInputStream(DMFileReader && reader_, bool enable_data_sharing_)
        : reader(std::move(reader_))
        , enable_data_sharing(enable_data_sharing_)
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

    Block read() override { return reader.read(); }

    Block readWithFilter(const IColumn::Filter & filter) override { return reader.readWithFilter(filter); }

private:
    friend class tests::DMFileMetaV2Test;
    DMFileReader reader;
    const bool enable_data_sharing;
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
    explicit DMFileBlockInputStreamBuilder(const Context & context);

    // Build the final stream ptr. LocalIndex will take effect.
    // Empty `rowkey_ranges` means not filter by rowkey
    SkippableBlockInputStreamPtr build(
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

    DMFileBlockInputStreamBuilder & setBitmapFilter(const BitmapFilterView & bitmap_filter_)
    {
        bitmap_filter.emplace(bitmap_filter_);
        return *this;
    }

    DMFileBlockInputStreamBuilder & setAnnQureyInfo(const ANNQueryInfoPtr & ann_query_info_)
    {
        ann_query_info = ann_query_info_;
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

    DMFileBlockInputStreamBuilder & setDMFilePackFilterResult(const DMFilePackFilterResultPtr & pack_filter_)
    {
        pack_filter = pack_filter_;
        return *this;
    }

    /**
     * @note To really enable the long term cache, you also need to ensure
     * ColumnCacheLongTerm is initialized in the global context.
     */
    DMFileBlockInputStreamBuilder & enableColumnCacheLongTerm(ColumnID pk_col_id_)
    {
        pk_col_id = pk_col_id_;
        return *this;
    }

private:
    DMFileBlockInputStreamPtr buildNoLocalIndex(
        const DMFilePtr & dmfile,
        const ColumnDefines & read_columns,
        const RowKeyRanges & rowkey_ranges,
        const ScanContextPtr & scan_context);


private:
    // These methods are called by the ctor

    DMFileBlockInputStreamBuilder & setFromSettings(const Settings & settings);

    DMFileBlockInputStreamBuilder & setCaches(
        const MarkCachePtr & mark_cache_,
        const MinMaxIndexCachePtr & index_cache_,
        const VectorIndexCachePtr & vector_index_cache_,
        const ColumnCacheLongTermPtr & column_cache_long_term_)
    {
        mark_cache = mark_cache_;
        index_cache = index_cache_;
        vector_index_cache = vector_index_cache_;
        column_cache_long_term = column_cache_long_term_;
        return *this;
    }

private:
    FileProviderPtr file_provider;

    // clean read

    bool enable_handle_clean_read = false;
    bool is_fast_scan = false;
    bool enable_del_clean_read = false;
    UInt64 max_data_version = std::numeric_limits<UInt64>::max();
    // packs filter (filter by pack index)
    IdSetPtr read_packs;
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

    DMFilePackFilterResultPtr pack_filter;

    ANNQueryInfoPtr ann_query_info = nullptr;

    VectorIndexCachePtr vector_index_cache;
    // Note: Currently thie field is assigned only for Stable streams, not available for ColumnFileBig
    std::optional<BitmapFilterView> bitmap_filter;

    // Note: column_cache_long_term is currently only filled when performing Vector Search.
    ColumnCacheLongTermPtr column_cache_long_term = nullptr;
    ColumnID pk_col_id = 0;
};

/**
 * Create a simple stream that read all blocks on default.
 * Only read one pack every time.
 * @param context Database context.
 * @param file DMFile pointer.
 * @param cols The columns to read. Empty means read all columns.
 * @return A shared pointer of an input stream
 */
SkippableBlockInputStreamPtr createSimpleBlockInputStream(
    const DB::Context & context,
    const DMFilePtr & file,
    ColumnDefines cols = {});

} // namespace DB::DM

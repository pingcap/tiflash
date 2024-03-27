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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

#include <memory>
namespace DB
{
class Context;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
namespace DM
{
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

    ~DMFileBlockInputStream()
    {
        if (enable_data_sharing)
        {
            DMFileReaderPool::instance().del(reader);
        }
    }

    String getName() const override { return "DMFile"; }

    Block getHeader() const override { return reader.getHeader(); }

    bool getSkippedRows(size_t & skip_rows) override { return reader.getSkippedRows(skip_rows); }

    Block read() override
    {
        return reader.read();
    }

private:
    DMFileReader reader;
    bool enable_data_sharing;
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
    DMFileBlockInputStreamBuilder & enableCleanRead(bool enable_handle_clean_read_, bool is_fast_scan_, bool enable_del_clean_read_, UInt64 max_data_version_)
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

private:
    // These methods are called by the ctor

    DMFileBlockInputStreamBuilder & setFromSettings(const Settings & settings)
    {
        enable_column_cache = settings.dt_enable_stable_column_cache;
        aio_threshold = settings.min_bytes_to_use_direct_io;
        max_read_buffer_size = settings.max_read_buffer_size;
        max_sharing_column_bytes_for_all = settings.dt_max_sharing_column_bytes_for_all;
        return *this;
    }
    DMFileBlockInputStreamBuilder & setCaches(const MarkCachePtr & mark_cache_, const MinMaxIndexCachePtr & index_cache_)
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
    size_t aio_threshold{};
    size_t max_read_buffer_size{};
    size_t rows_threshold_per_read = DMFILE_READ_ROWS_THRESHOLD;
    bool read_one_pack_every_time = false;
    size_t max_sharing_column_bytes_for_all = 0;
    String tracing_id;
};

/**
 * Create a simple stream that read all blocks on default.
 * Only read one pack every time.
 * @param context Database context.
 * @param file DMFile pointer.
 * @param cols The columns to read. Empty means read all columns.
 * @return A shared pointer of an input stream
 */
inline DMFileBlockInputStreamPtr createSimpleBlockInputStream(const DB::Context & context, const DMFilePtr & file, ColumnDefines cols = {})
{
    // disable clean read is needed, since we just want to read all data from the file, and we do not know about the column handle
    // enable read_one_pack_every_time_ is needed to preserve same block structure as the original file
    DMFileBlockInputStreamBuilder builder(context);
    if (cols.empty())
    {
        // turn into read all columns from file
        cols = file->getColumnDefines();
    }
    return builder
        .setRowsThreshold(DMFILE_READ_ROWS_THRESHOLD)
        .onlyReadOnePackEveryTime()
        .build(file, cols, DB::DM::RowKeyRanges{}, std::make_shared<ScanContext>());
}

} // namespace DM
} // namespace DB

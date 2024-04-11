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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/ScanContext.h>

namespace DB::DM
{

DMFileBlockInputStreamBuilder::DMFileBlockInputStreamBuilder(const Context & context)
    : file_provider(context.getFileProvider())
    , read_limiter(context.getReadLimiter())
{
    // init from global context
    const auto & global_context = context.getGlobalContext();
    setCaches(global_context.getMarkCache(), global_context.getMinMaxIndexCache());
    // init from settings
    setFromSettings(context.getSettingsRef());
}

DMFileBlockInputStreamPtr DMFileBlockInputStreamBuilder::build(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    const RowKeyRanges & rowkey_ranges,
    const ScanContextPtr & scan_context)
{
    RUNTIME_CHECK(
        dmfile->getStatus() == DMFile::Status::READABLE,
        dmfile->fileId(),
        magic_enum::enum_name(dmfile->getStatus()));

    // if `rowkey_ranges` is empty, we unconditionally read all packs
    // `rowkey_ranges` and `is_common_handle`  will only be useful in clean read mode.
    // It is safe to ignore them here.
    RUNTIME_CHECK_MSG(
        !(rowkey_ranges.empty() && enable_handle_clean_read),
        "rowkey ranges shouldn't be empty with clean-read enabled");

    bool is_common_handle = !rowkey_ranges.empty() && rowkey_ranges[0].is_common_handle;

    DMFilePackFilter pack_filter = DMFilePackFilter::loadFrom(
        dmfile,
        index_cache,
        /*set_cache_if_miss*/ true,
        rowkey_ranges,
        rs_filter,
        read_packs,
        file_provider,
        read_limiter,
        scan_context,
        tracing_id);

    bool enable_read_thread = SegmentReaderPoolManager::instance().isSegmentReader();

    if (!enable_read_thread || max_sharing_column_bytes_for_all <= 0)
    {
        // Disable data sharing.
        max_sharing_column_bytes_for_all = 0;
    }

    DMFileReader reader(
        dmfile,
        read_columns,
        is_common_handle,
        enable_handle_clean_read,
        enable_del_clean_read,
        is_fast_scan,
        max_data_version,
        std::move(pack_filter),
        mark_cache,
        enable_column_cache,
        column_cache,
        max_read_buffer_size,
        file_provider,
        read_limiter,
        rows_threshold_per_read,
        read_one_pack_every_time,
        tracing_id,
        max_sharing_column_bytes_for_all,
        scan_context,
        read_tag);

    return std::make_shared<DMFileBlockInputStream>(std::move(reader), max_sharing_column_bytes_for_all > 0);
}

DMFileBlockInputStreamPtr createSimpleBlockInputStream(
    const DB::Context & context,
    const DMFilePtr & file,
    ColumnDefines cols)
{
    // disable clean read is needed, since we just want to read all data from the file, and we do not know about the column handle
    // enable read_one_pack_every_time_ is needed to preserve same block structure as the original file
    DMFileBlockInputStreamBuilder builder(context);
    if (cols.empty())
    {
        // turn into read all columns from file
        cols = file->getColumnDefines();
    }
    return builder.setRowsThreshold(DMFILE_READ_ROWS_THRESHOLD)
        .onlyReadOnePackEveryTime()
        .build(file, cols, DB::DM::RowKeyRanges{}, std::make_shared<ScanContext>());
}

DMFileBlockInputStreamBuilder & DMFileBlockInputStreamBuilder::setFromSettings(const Settings & settings)
{
    enable_column_cache = settings.dt_enable_stable_column_cache;
    max_read_buffer_size = settings.max_read_buffer_size;
    max_sharing_column_bytes_for_all = settings.dt_max_sharing_column_bytes_for_all;
    return *this;
}

} // namespace DB::DM

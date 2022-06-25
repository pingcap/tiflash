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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>

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

DMFileBlockInputStreamPtr DMFileBlockInputStreamBuilder::build(const DMFilePtr & dmfile, const ColumnDefines & read_columns, const RowKeyRanges & rowkey_ranges)
{
    if (dmfile->getStatus() != DMFile::Status::READABLE)
        throw Exception(fmt::format(
                            "DMFile [{}] is expected to be in READABLE status, but: {}",
                            dmfile->fileId(),
                            DMFile::statusString(dmfile->getStatus())),
                        ErrorCodes::LOGICAL_ERROR);

    // if `rowkey_ranges` is empty, we unconditionally read all packs
    // `rowkey_ranges` and `is_common_handle`  will only be useful in clean read mode.
    // It is safe to ignore them here.
    if (unlikely(rowkey_ranges.empty() && enable_clean_read))
        throw Exception("rowkey ranges shouldn't be empty with clean-read enabled", ErrorCodes::LOGICAL_ERROR);

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
        tracing_id);

    DMFileReader reader(
        dmfile,
        read_columns,
        is_common_handle,
        enable_clean_read,
        max_data_version,
        std::move(pack_filter),
        mark_cache,
        enable_column_cache,
        column_cache,
        aio_threshold,
        max_read_buffer_size,
        file_provider,
        read_limiter,
        rows_threshold_per_read,
        read_one_pack_every_time,
        tracing_id);

    return std::make_shared<DMFileBlockInputStream>(std::move(reader));
}
} // namespace DB::DM

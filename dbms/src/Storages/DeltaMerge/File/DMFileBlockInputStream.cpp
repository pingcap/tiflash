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
#include <Storages/DeltaMerge/File/DMFileWithVectorIndexBlockInputStream.h>
#include <Storages/DeltaMerge/Filter/WithANNQueryInfo.h>
#include <Storages/DeltaMerge/Index/VectorIndex.h>
#include <Storages/DeltaMerge/ScanContext.h>

namespace DB::DM
{
DMFileBlockInputStreamBuilder::DMFileBlockInputStreamBuilder(const Context & context)
    : file_provider(context.getFileProvider())
    , read_limiter(context.getReadLimiter())
{
    // init from global context
    const auto & global_context = context.getGlobalContext();
    setCaches(
        global_context.getMarkCache(),
        global_context.getMinMaxIndexCache(),
        global_context.getVectorIndexCache());
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
        DMFile::statusString(dmfile->getStatus()));

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
        aio_threshold,
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

SkippableBlockInputStreamPtr DMFileBlockInputStreamBuilder::build2(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    const RowKeyRanges & rowkey_ranges,
    const ScanContextPtr & scan_context)
{
    auto fallback = [&]() {
        return build(dmfile, read_columns, rowkey_ranges, scan_context);
    };

    if (rs_filter == nullptr)
        return fallback();

    // Fast Scan and Clean Read does not affect our behavior. (TODO: Confirm?)
    // if (is_fast_scan || enable_del_clean_read || enable_handle_clean_read)
    //    return fallback();

    auto filter_with_ann = std::dynamic_pointer_cast<WithANNQueryInfo>(rs_filter);
    if (filter_with_ann == nullptr)
        return fallback();

    auto ann_query_info = filter_with_ann->ann_query_info;
    if (!ann_query_info)
        return fallback();

    if (!bitmap_filter.has_value())
        return fallback();

    // Fast check: ANNQueryInfo is available in the whole read path. However we may not reading vector column now.
    bool is_matching_ann_query = false;
    for (const auto & cd : read_columns)
    {
        if (cd.id == ann_query_info->column_id())
        {
            is_matching_ann_query = true;
            break;
        }
    }
    if (!is_matching_ann_query)
        return fallback();

    Block header_layout = toEmptyBlock(read_columns);

    // Copy out the vector column for later use. Copy is intentionally performed after the
    // fast check so that in fallback conditions we don't need unnecessary copies.
    std::optional<ColumnDefine> vec_column;
    ColumnDefines rest_columns{};
    for (const auto & cd : read_columns)
    {
        if (cd.id == ann_query_info->column_id())
            vec_column.emplace(cd);
        else
            rest_columns.emplace_back(cd);
    }

    // No vector index column is specified, just use the normal logic.
    if (!vec_column.has_value())
        return fallback();

    RUNTIME_CHECK(rest_columns.size() + 1 == read_columns.size(), rest_columns.size(), read_columns.size());

    const auto & vec_column_stat = dmfile->getColumnStat(vec_column->id);
    if (vec_column_stat.index_bytes == 0 || !vec_column_stat.vector_index.has_value())
        // Vector index is defined but does not exist on the data file,
        // or there is no data at all
        return fallback();

    // All check passed. Let's read via vector index.

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
    bool is_common_handle = !rowkey_ranges.empty() && rowkey_ranges[0].is_common_handle;

    DMFileReader rest_columns_reader(
        dmfile,
        rest_columns,
        is_common_handle,
        enable_handle_clean_read,
        enable_del_clean_read,
        is_fast_scan,
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
        tracing_id,
        enable_read_thread,
        scan_context);

    DMFileWithVectorIndexBlockInputStreamPtr reader = DMFileWithVectorIndexBlockInputStream::create(
        ann_query_info,
        dmfile,
        std::move(header_layout),
        std::move(rest_columns_reader),
        std::move(vec_column.value()),
        file_provider,
        read_limiter,
        scan_context,
        vector_index_cache,
        bitmap_filter.value(),
        tracing_id);

    return reader;
}

} // namespace DB::DM

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
        global_context.getVectorIndexCache(),
        global_context.getColumnCacheLongTerm());
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
        dmfile->getStatus() == DMFileStatus::READABLE,
        dmfile->fileId(),
        magic_enum::enum_name(dmfile->getStatus()));

    // if `rowkey_ranges` is empty, we unconditionally read all packs
    // `rowkey_ranges` and `is_common_handle`  will only be useful in clean read mode.
    // It is safe to ignore them here.
    RUNTIME_CHECK_MSG(
        !(rowkey_ranges.empty() && enable_handle_clean_read),
        "rowkey ranges shouldn't be empty with clean-read enabled");

    bool is_common_handle = !rowkey_ranges.empty() && rowkey_ranges[0].is_common_handle;

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
        *pack_filter,
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

SkippableBlockInputStreamPtr DMFileBlockInputStreamBuilder::tryBuildWithVectorIndex(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    const RowKeyRanges & rowkey_ranges,
    const ScanContextPtr & scan_context)
{
    auto fallback = [&]() {
        return build(dmfile, read_columns, rowkey_ranges, scan_context);
    };

    if (!ann_query_info)
        return fallback();

    if (!bitmap_filter.has_value())
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

    const IndexID ann_query_info_index_id = ann_query_info->index_id() > 0 //
        ? ann_query_info->index_id()
        : EmptyIndexID;
    if (!dmfile->isLocalIndexExist(vec_column->id, ann_query_info_index_id))
        // Vector index is defined but does not exist on the data file,
        // or there is no data at all
        return fallback();

    // All check passed. Let's read via vector index.

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
        *pack_filter,
        mark_cache,
        enable_column_cache,
        column_cache,
        max_read_buffer_size,
        file_provider,
        read_limiter,
        rows_threshold_per_read,
        false, // read multiple packs at once
        tracing_id,
        enable_read_thread,
        scan_context,
        ReadTag::Query);

    if (column_cache_long_term && pk_col_id)
        // ColumnCacheLongTerm is only filled in Vector Search.
        rest_columns_reader.setColumnCacheLongTerm(column_cache_long_term, pk_col_id);

    DMFileWithVectorIndexBlockInputStreamPtr reader = DMFileWithVectorIndexBlockInputStream::create(
        ann_query_info,
        dmfile,
        std::move(header_layout),
        std::move(rest_columns_reader),
        std::move(vec_column.value()),
        scan_context,
        vector_index_cache,
        bitmap_filter.value(),
        tracing_id);

    return reader;
}

} // namespace DB::DM

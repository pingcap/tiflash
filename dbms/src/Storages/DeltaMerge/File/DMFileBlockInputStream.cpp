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

#include <Common/config.h> // For ENABLE_CLARA
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Perf.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Reader.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/DMFileInputStream.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/DistanceProjectionInputStream.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/ScanContext.h>

#if ENABLE_CLARA
#include <Storages/DeltaMerge/Index/FullTextIndex/Perf.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Reader.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/BruteScoreInputStream.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/DMFileInputStream.h>
#endif
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
        global_context.getColumnCacheLongTerm());
    // init from settings
    setFromSettings(context.getSettingsRef());
}

DMFileBlockInputStreamPtr DMFileBlockInputStreamBuilder::buildNoLocalIndex(
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

    // If pack_filter is not set, load from EMPTY_RS_OPERATOR.
    if (!pack_filter)
    {
        pack_filter = DMFilePackFilter::loadFrom(
            index_cache,
            file_provider,
            read_limiter,
            scan_context,
            dmfile,
            true,
            rowkey_ranges,
            EMPTY_RS_OPERATOR,
            read_packs,
            tracing_id);
    }

    DMFileReader reader(
        dmfile,
        read_columns,
        is_common_handle,
        enable_handle_clean_read,
        enable_del_clean_read,
        is_fast_scan,
        max_data_version,
        pack_filter,
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

SkippableBlockInputStreamPtr createSimpleBlockInputStream(
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

SkippableBlockInputStreamPtr DMFileBlockInputStreamBuilder::build(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    const RowKeyRanges & rowkey_ranges,
    const ScanContextPtr & scan_context)
{
    // Note: this file may not have index built
    {
#if ENABLE_CLARA
        if (fts_index_ctx)
            return buildForFullTextIndex(dmfile, read_columns, rowkey_ranges, scan_context);
#endif
        if (vec_index_ctx)
            return buildForVectorIndex(dmfile, read_columns, rowkey_ranges, scan_context);
    }
    return buildNoLocalIndex(dmfile, read_columns, rowkey_ranges, scan_context);
}

SkippableBlockInputStreamPtr DMFileBlockInputStreamBuilder::buildForVectorIndex(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    const RowKeyRanges & rowkey_ranges,
    const ScanContextPtr & scan_context)
{
    RUNTIME_CHECK(vec_index_ctx != nullptr);
    RUNTIME_CHECK(read_columns.size() == vec_index_ctx->col_defs->size());

    auto fallback = [&]() -> SkippableBlockInputStreamPtr {
        vec_index_ctx->perf->n_from_dmf_noindex += 1;
        if (!vec_index_ctx->ann_query_info->enable_distance_proj())
        {
            return buildNoLocalIndex(dmfile, read_columns, rowkey_ranges, scan_context);
        }

        // if enable_distance_proj, the result of read will have not distance but we need it, so return a
        // DistanceProjectionInputStream to calc it.
        auto stream
            = buildNoLocalIndex(dmfile, *vec_index_ctx->dis_ctx->col_defs_no_index, rowkey_ranges, scan_context);

        return DistanceProjectionInputStream::create(stream, vec_index_ctx);
    };

    auto local_index = dmfile->getLocalIndex( //
        vec_index_ctx->vec_col_id,
        vec_index_ctx->ann_query_info->index_id());
    if (!local_index.has_value())
        // Vector index is defined but does not exist on the data file,
        // or there is no data at all
        return fallback();

    RUNTIME_CHECK(local_index->index_props().kind() == dtpb::IndexFileKind::VECTOR_INDEX);

    bool enable_read_thread = SegmentReaderPoolManager::instance().isSegmentReader();
    bool is_common_handle = !rowkey_ranges.empty() && rowkey_ranges[0].is_common_handle;

    // If pack_filter is not set, load from EMPTY_RS_OPERATOR.
    if (!pack_filter)
    {
        pack_filter = DMFilePackFilter::loadFrom(
            index_cache,
            file_provider,
            read_limiter,
            scan_context,
            dmfile,
            true,
            rowkey_ranges,
            EMPTY_RS_OPERATOR,
            read_packs,
            tracing_id);
    }

    DMFileReader rest_columns_reader(
        dmfile,
        *vec_index_ctx->rest_col_defs,
        is_common_handle,
        enable_handle_clean_read,
        enable_del_clean_read,
        is_fast_scan,
        max_data_version,
        pack_filter,
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
        read_tag);

    if (column_cache_long_term && pk_col_id)
        // ColumnCacheLongTerm is only filled in Vector Search.
        rest_columns_reader.setColumnCacheLongTerm(column_cache_long_term, pk_col_id);

    vec_index_ctx->perf->n_from_dmf_index += 1;
    return DMFileInputStreamProvideVectorIndex::create( //
        vec_index_ctx,
        dmfile,
        std::move(rest_columns_reader));
}

#if ENABLE_CLARA
SkippableBlockInputStreamPtr DMFileBlockInputStreamBuilder::buildForFullTextIndex(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    const RowKeyRanges & rowkey_ranges,
    const ScanContextPtr & scan_context)
{
    RUNTIME_CHECK(fts_index_ctx != nullptr);
    RUNTIME_CHECK(read_columns.size() == fts_index_ctx->schema->size());

    auto fallback = [&]() {
        fts_index_ctx->perf->n_from_dmf_noindex += 1;
        fts_index_ctx->perf->rows_from_dmf_noindex += dmfile->getRows();

        auto full_col_stream = buildNoLocalIndex( //
            dmfile,
            *fts_index_ctx->noindex_read_schema.get(),
            rowkey_ranges,
            scan_context);
        RUNTIME_CHECK(full_col_stream != nullptr);

        return FullTextBruteScoreInputStream::create(fts_index_ctx, full_col_stream);
    };

    auto local_index = dmfile->getLocalIndex( //
        fts_index_ctx->fts_query_info->columns()[0].column_id(),
        fts_index_ctx->fts_query_info->index_id());
    if (!local_index.has_value())
        // Vector index is defined but does not exist on the data file,
        // or there is no data at all
        return fallback();

    RUNTIME_CHECK(local_index->index_props().kind() == dtpb::IndexFileKind::FULLTEXT_INDEX);

    bool enable_read_thread = SegmentReaderPoolManager::instance().isSegmentReader();
    bool is_common_handle = !rowkey_ranges.empty() && rowkey_ranges[0].is_common_handle;

    // If pack_filter is not set, load from EMPTY_RS_OPERATOR.
    if (!pack_filter)
    {
        pack_filter = DMFilePackFilter::loadFrom(
            index_cache,
            file_provider,
            read_limiter,
            scan_context,
            dmfile,
            true,
            rowkey_ranges,
            EMPTY_RS_OPERATOR,
            read_packs,
            tracing_id);
    }

    DMFileReader rest_columns_reader(
        dmfile,
        *fts_index_ctx->rest_col_schema,
        is_common_handle,
        enable_handle_clean_read,
        enable_del_clean_read,
        is_fast_scan,
        max_data_version,
        pack_filter,
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
        read_tag);

    if (column_cache_long_term && pk_col_id)
        // ColumnCacheLongTerm is only filled in Vector Search.
        rest_columns_reader.setColumnCacheLongTerm(column_cache_long_term, pk_col_id);

    fts_index_ctx->perf->n_from_dmf_index += 1;
    fts_index_ctx->perf->rows_from_dmf_index += dmfile->getRows();

    return DMFileInputStreamProvideFullTextIndex::create( //
        fts_index_ctx,
        dmfile,
        std::move(rest_columns_reader));
}
#endif

} // namespace DB::DM

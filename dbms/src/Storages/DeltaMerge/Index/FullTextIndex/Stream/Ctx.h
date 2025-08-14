// Copyright 2025 PingCAP, Inc.
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

#include <Common/config.h>

#if ENABLE_CLARA
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider_fwd.h>
#include <Storages/DeltaMerge/DMContext_fwd.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Perf_fwd.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Reader_fwd.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx_fwd.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache_fwd.h>
#include <Storages/DeltaMerge/ReadMode.h>
#include <clara_fts/src/brute_searcher.rs.h>

namespace DB::DM
{

/// Some commonly shared things through out a single full text search process.
/// Its expected lifetime >= FullTextIndexInputStream.
struct FullTextIndexStreamCtx
{
    /// Notice: name must not be used because ExchangeReceiver expects some different names
    static const ColumnDefine VIRTUAL_SCORE_CD;

    // Note: Currently FTSQueryInfo always asks Storage layer to return
    // an additional score column, with columnId=-2050.

    const LocalIndexCachePtr index_cache_light; // nullable
    const LocalIndexCachePtr index_cache_heavy; // nullable
    const FTSQueryInfoPtr fts_query_info;

    /// This is what TiDB asks TiFlashTableScan to return (i.e. TableScan's schema)
    /// It always contains the score column at last. The score column must match the constant
    //// VIRTUAL_SCORE_CD.
    /// It may, or may not contain the FTS column, depends on whether user asks for it.
    const ColumnDefinesPtr schema;
    /// The FTS column index in the `schema`, if exists. This may be `nullopt`
    /// if the FTS column is not in the schema.
    const std::optional<size_t> fts_idx_in_schema;
    /// The ColumnDefinition of the FTS column when it is presented in `schema`.
    /// It could be `nullopt` if the FTS column is not in the schema.
    const std::optional<ColumnDefine> fts_cd_in_schema;
    /// The ColumnDefinition of the score column in `schema`.
    /// It should be the same as `VIRTUAL_SCORE_CD` except the name (which may be changed by ExchangeReceiver).
    const ColumnDefine score_cd_in_schema;
    /// Roughtly, `rest_col_schema=schema-score_col-optional_fts_col`. This is used
    /// to fill the content of the block after we have finished FullTextSearch.
    /// The column order must be the same as in `schema`.
    const ColumnDefinesPtr rest_col_schema;
    /// This is the schema used when Stable/Delta does not have index and must calculate
    /// a score based on a Vector Column. Roughly, `noindex_read_schema=rest_col_schema+fts_col`.
    /// The last column must be the FTS column.
    const ColumnDefinesPtr noindex_read_schema;

    /// Constructed directly from `schema`.
    /// This is what TiDB asks TiFlashTableScan to return (i.e. TableScan's schema).
    const Block schema_as_header;

    // ============================================================
    // Fields below are for accessing ColumnFile
    const IColumnFileDataProviderPtr data_provider;

    /// WARN: Do not use this in cases other than building a ColumnFileInputStream.
    /// Because this field is not set in DMFile only tests.
    /// FIXME: This pointer is also definitely dangerous. We cannot guarantee the lifetime.
    const DMContext * dm_context;

    const ReadTag read_tag;
    // ============================================================

    /// Note: This field is also available in dm_context. However dm_context could be null in tests.
    const String tracing_id;

    // ============================================================
    // Fields below are mutable and shared without any lock, because our input streams
    // will only operate one by one, and there is no contention in each read()
    const FullTextIndexPerfPtr perf; // perf is modifyable
    /// reused in each read()
    IColumn::Filter filter;
    /// reused in each read()
    rust::String text_value;
    /// reused in each read()
    rust::Vec<ClaraFTS::ScoredResult> results;
    /// reused in each read() when a scoring on-demand search is needed.
    /// Use ensureBruteScoredSearcher() for an easy access.
    std::optional<rust::Box<ClaraFTS::BruteScoredSearcher>> brute_searcher;
    // ============================================================

    static FullTextIndexStreamCtxPtr create(
        const LocalIndexCachePtr & index_cache_light_, // nullable
        const LocalIndexCachePtr & index_cache_heavy_, // nullable
        const FTSQueryInfoPtr & fts_query_info_,
        const ColumnDefinesPtr & schema_,
        const IColumnFileDataProviderPtr & data_provider_, // Must provide for this interface
        const DMContext & dm_context_,
        const ReadTag & read_tag_);

    // Only used in tests!
    static FullTextIndexStreamCtxPtr createForStableOnlyTests(
        const FTSQueryInfoPtr & fts_query_info_,
        const ColumnDefinesPtr & schema_,
        const LocalIndexCachePtr & index_cache_light_ = nullptr);

    // Helper functions
    ClaraFTS::BruteScoredSearcher & ensureBruteScoredSearcher();
};

} // namespace DB::DM
#endif

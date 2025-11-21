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

#include <Common/config.h>

#if ENABLE_CLARA
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Perf.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>


namespace DB::DM
{

const ColumnDefine FullTextIndexStreamCtx::VIRTUAL_SCORE_CD = ColumnDefine( //
    -2050,
    "_INTERNAL_FTS_SCORE",
    DataTypeFactory::instance().getOrSet("Float32"));

namespace details
{

auto buildCtx(
    const LocalIndexCachePtr & index_cache_light, // nullable
    const LocalIndexCachePtr & index_cache_heavy, // nullable
    const FTSQueryInfoPtr & fts_query_info,
    const ColumnDefinesPtr & schema,
    const IColumnFileDataProviderPtr & data_provider,
    const DMContext * dm_context,
    const String & tracing_id,
    const ReadTag & read_tag)
{
    RUNTIME_CHECK(fts_query_info != nullptr);
    RUNTIME_CHECK(schema != nullptr);

    // Currently only TopK is supported.
    RUNTIME_CHECK(fts_query_info->query_type() == tipb::FTSQueryTypeWithScore);
    RUNTIME_CHECK(fts_query_info->has_index_id());
    RUNTIME_CHECK(fts_query_info->columns().size() == 1);

    RUNTIME_CHECK(!schema->empty());
    auto score_cd_in_schema = schema->back();
    RUNTIME_CHECK(score_cd_in_schema.id == FullTextIndexStreamCtx::VIRTUAL_SCORE_CD.id);
    RUNTIME_CHECK(score_cd_in_schema.type->equals(*FullTextIndexStreamCtx::VIRTUAL_SCORE_CD.type));

    auto rest_col_schema = std::make_shared<ColumnDefines>();
    rest_col_schema->reserve(schema->size() - 1);

    ColumnID fts_col_id = fts_query_info->columns()[0].column_id();
    std::optional<size_t> fts_idx_in_schema;
    std::optional<ColumnDefine> fts_cd_in_schema;
    for (size_t i = 0, i_max = schema->size(); i < i_max; ++i)
    {
        const auto & cd = schema->at(i);
        if (cd.id == fts_col_id)
        {
            fts_idx_in_schema.emplace(i);
            fts_cd_in_schema.emplace(cd);
        }
        if (cd.id != score_cd_in_schema.id && cd.id != fts_col_id)
        {
            rest_col_schema->emplace_back(cd);
        }
    }

    auto noindex_read_schema = std::make_shared<ColumnDefines>();
    noindex_read_schema->reserve(rest_col_schema->size() + 1);
    noindex_read_schema->insert(noindex_read_schema->end(), rest_col_schema->begin(), rest_col_schema->end());
    {
        auto column_info = TiDB::toTiDBColumnInfo(fts_query_info->columns()[0]);
        auto data_type = getDataTypeByColumnInfo(column_info);
        RUNTIME_CHECK(removeNullable(data_type)->isString());
        // Use an arbitrary column name for the noindex schema, because it will be swallowed anyway.
        noindex_read_schema->emplace_back(fts_col_id, "_INTERNAL_FTS_COLUMN", data_type);
    }

    return std::make_shared<FullTextIndexStreamCtx>(FullTextIndexStreamCtx{
        .index_cache_light = index_cache_light,
        .index_cache_heavy = index_cache_heavy,
        .fts_query_info = fts_query_info,
        .schema = schema,
        .fts_idx_in_schema = fts_idx_in_schema,
        .fts_cd_in_schema = std::move(fts_cd_in_schema),
        .score_cd_in_schema = std::move(score_cd_in_schema),
        .rest_col_schema = rest_col_schema,
        .noindex_read_schema = noindex_read_schema,
        .schema_as_header = toEmptyBlock(*schema),
        .data_provider = data_provider,
        .dm_context = dm_context,
        .read_tag = read_tag,
        .tracing_id = tracing_id,
        .perf = FullTextIndexPerf::create(),
    });
}

} // namespace details


FullTextIndexStreamCtxPtr FullTextIndexStreamCtx::create(
    const LocalIndexCachePtr & index_cache_light, // nullable
    const LocalIndexCachePtr & index_cache_heavy, // nullable
    const FTSQueryInfoPtr & fts_query_info,
    const ColumnDefinesPtr & schema,
    const IColumnFileDataProviderPtr & data_provider,
    const DMContext & dm_context,
    const ReadTag & read_tag)
{
    RUNTIME_CHECK(data_provider != nullptr);
    return details::buildCtx(
        index_cache_light,
        index_cache_heavy,
        fts_query_info,
        schema,
        data_provider,
        &dm_context,
        dm_context.tracing_id,
        read_tag);
}

FullTextIndexStreamCtxPtr FullTextIndexStreamCtx::createForStableOnlyTests(
    const FTSQueryInfoPtr & fts_query_info,
    const ColumnDefinesPtr & schema,
    const LocalIndexCachePtr & index_cache_light)
{
    return details::buildCtx(
        /* index_cache_light */ index_cache_light,
        /* index_cache_heavy */ nullptr,
        fts_query_info,
        schema,
        /* data_provider */ nullptr,
        /* dm_context */ nullptr,
        /* tracing_id */ "",
        /* read_tag */ ReadTag::Internal);
}

ClaraFTS::BruteScoredSearcher & FullTextIndexStreamCtx::ensureBruteScoredSearcher()
{
    if unlikely (!brute_searcher.has_value())
    {
        brute_searcher.emplace(ClaraFTS::new_brute_scored_searcher( //
            fts_query_info->query_tokenizer(),
            fts_query_info->query_text()));
    }
    return *brute_searcher.value();
}

} // namespace DB::DM
#endif

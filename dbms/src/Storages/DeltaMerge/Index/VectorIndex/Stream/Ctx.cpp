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

#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Perf.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx.h>

namespace DB::DM
{

VectorIndexStreamCtxPtr VectorIndexStreamCtx::create(
    const LocalIndexCachePtr & index_cache_light, // nullable
    const LocalIndexCachePtr & index_cache_heavy, // nullable
    const ANNQueryInfoPtr & ann_query_info,
    const ColumnDefinesPtr & col_defs,
    const IColumnFileDataProviderPtr & data_provider,
    const DMContext & dm_context,
    const ReadTag & read_tag)
{
    RUNTIME_CHECK(ann_query_info != nullptr);
    RUNTIME_CHECK(data_provider != nullptr);
    RUNTIME_CHECK(!col_defs->empty());

    std::optional<ColumnDefine> vec_cd;
    auto rest_columns = std::make_shared<ColumnDefines>();
    rest_columns->reserve(col_defs->size() - 1);
    for (const auto & cd : *col_defs)
    {
        if (cd.id == ann_query_info->deprecated_column_id())
            vec_cd.emplace(cd);
        else
            rest_columns->emplace_back(cd);
    }
    RUNTIME_CHECK(vec_cd.has_value());
    RUNTIME_CHECK(rest_columns->size() + 1 == col_defs->size());

    auto header = toEmptyBlock(*col_defs);

    return std::make_shared<VectorIndexStreamCtx>(VectorIndexStreamCtx{
        .index_cache_light = index_cache_light,
        .index_cache_heavy = index_cache_heavy,
        .ann_query_info = ann_query_info,
        .col_defs = col_defs,
        .vec_cd = *vec_cd,
        .rest_col_defs = rest_columns,
        .header = header,
        .data_provider = data_provider,
        .dm_context = &dm_context,
        .read_tag = read_tag,
        .tracing_id = dm_context.tracing_id,
        .perf = VectorIndexPerf::create(),
    });
}

VectorIndexStreamCtxPtr VectorIndexStreamCtx::createForStableOnlyTests(
    const ANNQueryInfoPtr & ann_query_info,
    const ColumnDefinesPtr & col_defs,
    const LocalIndexCachePtr & index_cache_light)
{
    RUNTIME_CHECK(ann_query_info != nullptr);
    RUNTIME_CHECK(!col_defs->empty());

    std::optional<ColumnDefine> vec_cd;
    auto rest_columns = std::make_shared<ColumnDefines>();
    rest_columns->reserve(col_defs->size() - 1);
    for (const auto & cd : *col_defs)
    {
        if (cd.id == ann_query_info->deprecated_column_id())
            vec_cd.emplace(cd);
        else
            rest_columns->emplace_back(cd);
    }
    RUNTIME_CHECK(vec_cd.has_value());
    RUNTIME_CHECK(rest_columns->size() + 1 == col_defs->size());

    auto header = toEmptyBlock(*col_defs);

    return std::make_shared<VectorIndexStreamCtx>(VectorIndexStreamCtx{
        .index_cache_light = index_cache_light,
        .index_cache_heavy = nullptr,
        .ann_query_info = ann_query_info,
        .col_defs = col_defs,
        .vec_cd = *vec_cd,
        .rest_col_defs = rest_columns,
        .header = header,
        .data_provider = nullptr,
        .dm_context = nullptr,
        .read_tag = ReadTag::Internal,
        .tracing_id = "",
        .perf = VectorIndexPerf::create(),
    });
}

} // namespace DB::DM

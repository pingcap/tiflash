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

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionsVector.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Index/VectorIndex/CommonUtil.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Perf.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>

namespace DB::DM
{

const ColumnDefine VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD
    = ColumnDefine(-2000, "_INTERNAL_VEC_SEARCH_DISTANCE", DataTypeFactory::instance().get("Nullable(Float32)"));

void VectorIndexStreamCtx::prepareForDistanceProjStream()
{
    RUNTIME_CHECK(dis_ctx.has_value());
    // if the value has been set, just return and re-use them.
    if (dis_ctx->is_prepared)
        return;

    auto distance_func
        = getVecDistanceFnFromMetric<Float32>(ann_query_info->distance_metric(), dm_context->global_context);
    dis_ctx->distance_fn = distance_func;

    // check if the fn-retrun-type is equal to distance_column type for DistanceProjectionInputStream.transform.
    // the last position of col_defs_no_index is a vector type.
    size_t last_idx = dis_ctx->col_defs_no_index->size() - 1;
    const auto & vec_type = (*dis_ctx->col_defs_no_index)[last_idx].type;
    auto result_type = distance_func->getReturnTypeImpl({vec_type, vec_type});
    RUNTIME_CHECK(dis_ctx->dis_cd.type->equals(*result_type));

    const auto & ref_vec_bytes = ann_query_info->ref_vec_f32();
    RUNTIME_CHECK(ref_vec_bytes.size() >= sizeof(UInt32));
    const auto ref_vec_size = readLittleEndian<UInt32>(ref_vec_bytes.data());
    RUNTIME_CHECK(ref_vec_bytes.size() == sizeof(UInt32) + ref_vec_size * sizeof(Float32));

    auto ref_vec_col = ColumnArray::create(ColumnFloat32::create());
    ref_vec_col->insertData(ref_vec_bytes.data() + sizeof(UInt32), ref_vec_size * sizeof(Float32));

    dis_ctx->ref_vec_col = std::move(ref_vec_col);
    dis_ctx->is_prepared = true;
}

namespace details
{

auto buildCtx(
    const LocalIndexCachePtr & index_cache_light, // nullable
    const LocalIndexCachePtr & index_cache_heavy, // nullable
    const ANNQueryInfoPtr & ann_query_info,
    const ColumnDefinesPtr & col_defs,
    const IColumnFileDataProviderPtr & data_provider,
    const DMContext * dm_context,
    const String & tracing_id,
    const ReadTag & read_tag)
{
    RUNTIME_CHECK(ann_query_info != nullptr);
    RUNTIME_CHECK(!col_defs->empty());

    ColumnID vector_column_id = getVectorColumnID(ann_query_info);

    std::optional<ColumnDefine> vec_cd;
    std::optional<ColumnDefine> dis_cd;
    std::optional<size_t> vec_idx;
    auto rest_columns = std::make_shared<ColumnDefines>();
    rest_columns->reserve(col_defs->size() - 1);
    ColumnDefinesPtr col_defs_no_index = nullptr; // Only assigned in distance_proj enabled case

    if (!ann_query_info->enable_distance_proj())
    {
        for (size_t i = 0, i_max = col_defs->size(); i < i_max; ++i)
        {
            const auto & cd = (*col_defs)[i];
            RUNTIME_CHECK_MSG(
                cd.id != VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD.id,
                "got distance column but enable_distance_proj is false, please check the creation of plan.");

            if (cd.id == vector_column_id)
            {
                vec_cd.emplace(cd);
                vec_idx.emplace(i);
            }
            else
            {
                rest_columns->emplace_back(cd);
            }
        }
        RUNTIME_CHECK(vec_cd.has_value());
    }
    else
    {
        col_defs_no_index = std::make_shared<ColumnDefines>();
        col_defs_no_index->reserve(col_defs->size());

        // When distance_proj is enabled, the last column is the distance column and there should be no vector column
        // in any position.
        for (size_t i = 0, i_max = col_defs->size() - 1; i < i_max; ++i)
        {
            const auto & cd = (*col_defs)[i];
            // We will only replace the last position of col_defs with distance column.
            // Other position which equal to VIRTUAL_DISTANCE_CD.id is illegal.
            RUNTIME_CHECK_MSG(
                cd.id != VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD.id,
                "got more than one distance column, please check the creation of plan.");
            RUNTIME_CHECK(cd.id != vector_column_id); // TiDB must not pass any vector column
            rest_columns->emplace_back(cd);
            col_defs_no_index->emplace_back(cd);
        }

        // distance column is added into the last position in schema, so need to check if the position is right.
        RUNTIME_CHECK_MSG(
            col_defs->back().id == VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD.id,
            "enable_distance_proj is true but the last position is not distance column, please check the creation of "
            "plan.");

        // distance column is expected as Nullable.
        RUNTIME_CHECK_MSG(
            col_defs->back().type->isNullable(),
            "the distance column is expected as Nullable(Float32) but got {}, please check the creation of distance "
            "column in TiDB.",
            col_defs->back().type->getName());

        dis_cd.emplace(col_defs->back());
        // If enable_distance_proj is true, tidb will remove the vector column and add distance column in table_scan plan.
        // col_defs_no_index will be used when index is not build, where we still need to read a vector column.
        auto vec_column = TiDB::toTiDBColumnInfo(ann_query_info->column());
        col_defs_no_index->emplace_back(ColumnDefine(
            vector_column_id,
            "_INTERNAL_VEC_COL", // The name has no effect because the column will be replaced by VIRTUAL_DISTANCE_CD.
            getDataTypeByColumnInfo(vec_column)));
    }

    RUNTIME_CHECK(vec_idx.has_value() != ann_query_info->enable_distance_proj());
    RUNTIME_CHECK(vec_cd.has_value() != dis_cd.has_value());
    RUNTIME_CHECK(rest_columns->size() + 1 == col_defs->size());

    auto header = toEmptyBlock(*col_defs);

    // if enable_distance_proj is true, that means we don't need to read vector column by vector index.
    // Some values ​​need to be prepared in VectorIndexStreamCtx, for example:
    // dis_cd: save the distance column info for reading, as there may be different content when tiflash node communicate each other.
    // col_defs_no_index: replace the distance column with vector column for reading when vector index not build up.
    // func: indicate which function need to execute in DistanceProjectionInputStream::transform.
    if (ann_query_info->enable_distance_proj())
    {
        return std::make_shared<VectorIndexStreamCtx>(VectorIndexStreamCtx{
            .index_cache_light = index_cache_light,
            .index_cache_heavy = index_cache_heavy,
            .ann_query_info = ann_query_info,
            .col_defs = col_defs,
            .vec_cd = std::move(vec_cd),
            .dis_ctx = VectorIndexStreamCtx::DistanceProjectionCtx{ //
                .col_defs_no_index = std::move(col_defs_no_index), 
                .dis_cd = std::move(dis_cd.value()),
                .is_prepared = false,
            },
            .vec_col_id = vector_column_id,
            .rest_col_defs = rest_columns,
            .header = header,
            .data_provider = data_provider,
            .dm_context = dm_context,
            .read_tag = read_tag,
            .tracing_id = tracing_id,
            .perf = VectorIndexPerf::create(),
        });
    }
    return std::make_shared<VectorIndexStreamCtx>(VectorIndexStreamCtx{
        .index_cache_light = index_cache_light,
        .index_cache_heavy = index_cache_heavy,
        .ann_query_info = ann_query_info,
        .col_defs = col_defs,
        .vec_col_idx = std::move(vec_idx),
        .vec_cd = std::move(vec_cd),
        .vec_col_id = vector_column_id,
        .rest_col_defs = rest_columns,
        .header = header,
        .data_provider = data_provider,
        .dm_context = dm_context,
        .read_tag = read_tag,
        .tracing_id = tracing_id,
        .perf = VectorIndexPerf::create(),
    });
}

} // namespace details

VectorIndexStreamCtxPtr VectorIndexStreamCtx::create(
    const LocalIndexCachePtr & index_cache_light, // nullable
    const LocalIndexCachePtr & index_cache_heavy, // nullable
    const ANNQueryInfoPtr & ann_query_info,
    const ColumnDefinesPtr & col_defs,
    const IColumnFileDataProviderPtr & data_provider,
    const DMContext & dm_context,
    const ReadTag & read_tag)
{
    RUNTIME_CHECK(data_provider != nullptr);
    return details::buildCtx(
        index_cache_light,
        index_cache_heavy,
        ann_query_info,
        col_defs,
        data_provider,
        &dm_context,
        dm_context.tracing_id,
        read_tag);
}

VectorIndexStreamCtxPtr VectorIndexStreamCtx::createForStableOnlyTests(
    const ANNQueryInfoPtr & ann_query_info,
    const ColumnDefinesPtr & col_defs,
    const LocalIndexCachePtr & index_cache_light)
{
    return details::buildCtx(
        /* index_cache_light */ index_cache_light,
        /* index_cache_heavy */ nullptr,
        ann_query_info,
        col_defs,
        /* data_provider */ nullptr,
        /* dm_context */ nullptr,
        /* tracing_id */ "",
        /* read_tag */ ReadTag::Internal);
}

} // namespace DB::DM
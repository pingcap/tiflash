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

#include <Functions/IFunction.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider_fwd.h>
#include <Storages/DeltaMerge/DMContext_fwd.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Perf_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Reader_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx_fwd.h>
#include <Storages/DeltaMerge/ReadMode.h>

namespace DB::DM
{

/// Some commonly shared things through out a single vector search process.
/// Its expected lifetime >= VectorIndexInputStream.
struct VectorIndexStreamCtx
{
    struct DistanceProjectionCtx
    {
        // This field will be fill when enable_distance_proj is true and vector index not build up. when enable_distance_proj is true, col_defs have no vector column, so
        // retrieve back the vector column to the last position for reading vector column without vector index.
        const ColumnDefinesPtr col_defs_no_index;
        // This field is the extra distance column in the TableScan's schema.
        // Although distance column schema is fixed from TiDB side, when different tiflash nodes pass the read column information, distance col define may be set to other names,
        // so the column information still needs to be saved here.
        const ColumnDefine dis_cd;

        // A column contains 1 row, which is the ref_vector in the AnnQueryInfo. This is used for calculating the distance when index is not built.
        // This field is not assigned by default, and only constructed when it is being used.
        ColumnPtr ref_vec_col; // nullable
        // This field is not assigned by default, and only constructed when it is being used.
        FunctionPtr distance_fn; // nullable

        // Indicate if this ctx has been filled. The value in DistanceProjectionCtx is the same for each process.
        bool is_prepared = false;
    };

    // constants for vector search to fill the distane column.
    static const ColumnDefine VIRTUAL_DISTANCE_CD;
    const LocalIndexCachePtr index_cache_light; // nullable
    const LocalIndexCachePtr index_cache_heavy; // nullable
    const ANNQueryInfoPtr ann_query_info;
    const ColumnDefinesPtr col_defs;
    // Note that when enable_distance_proj, vec column will not exist in the TableScan's schema and this field will be nullopt.
    const std::optional<size_t> vec_col_idx;

    // Note that when enable_distance_proj, vec column will not exist in the TableScan's schema and this field will be nullopt.
    const std::optional<ColumnDefine> vec_cd;

    // will be set if enable_distance_proj is true.
    std::optional<DistanceProjectionCtx> dis_ctx;

    const ColumnID vec_col_id;
    const ColumnDefinesPtr rest_col_defs;
    const Block header;

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
    const VectorIndexPerfPtr perf; // perf is modifyable
    std::vector<Float32> vector_value;
    /// reused in each read()
    IColumn::Filter filter;
    // ============================================================


    static VectorIndexStreamCtxPtr create(
        const LocalIndexCachePtr & index_cache_light_,
        const LocalIndexCachePtr & index_cache_heavy_,
        const ANNQueryInfoPtr & ann_query_info_,
        const ColumnDefinesPtr & col_defs_,
        const IColumnFileDataProviderPtr & data_provider_, // Must provide for this interface
        const DMContext & dm_context_,
        const ReadTag & read_tag_);

    // Only used in tests!
    static VectorIndexStreamCtxPtr createForStableOnlyTests(
        const ANNQueryInfoPtr & ann_query_info_,
        const ColumnDefinesPtr & col_defs_,
        const LocalIndexCachePtr & index_cache_light_ = nullptr);

    void prepareForDistanceProjStream();
};

} // namespace DB::DM

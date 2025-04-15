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

#include <Common/Exception.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Reader_fwd.h>
#include <Storages/KVStore/Types.h>
#include <VectorSearch/USearch.h>
#include <tipb/executor.pb.h>

namespace DB::DM
{

// Due to the discarding of column_id in the vector index, the value assigned to ann_query_info may be different
// in different versions of tidb. For compatibility, use this function to get column_id.
// For field changes, see: https://github.com/pingcap/tipb/pull/358
inline ColumnID getVectorColumnID(const ANNQueryInfoPtr & ann_query_info)
{
    // Prioritize obtaining the deprecated_column_id because this field is set in
    // the old version of tidb and is considered to be a valid value.
    if (ann_query_info->has_deprecated_column_id())
        return ann_query_info->deprecated_column_id();
    else if (ann_query_info->has_column())
        return ann_query_info->column().column_id();
    else
        throw DB::Exception("Can't get vector column id from tipb::ANNQueryInfo, please check the version of TiDB.");
}

inline unum::usearch::metric_kind_t getUSearchMetricKind(tipb::VectorDistanceMetric d)
{
    switch (d)
    {
    case tipb::VectorDistanceMetric::INNER_PRODUCT:
        return unum::usearch::metric_kind_t::ip_k;
    case tipb::VectorDistanceMetric::COSINE:
        return unum::usearch::metric_kind_t::cos_k;
    case tipb::VectorDistanceMetric::L2:
        return unum::usearch::metric_kind_t::l2sq_k;
    default:
        // Specifically, L1 is currently unsupported by usearch.

        RUNTIME_CHECK_MSG( //
            false,
            "Unsupported vector distance {}",
            tipb::VectorDistanceMetric_Name(d));
    }
}

} // namespace DB::DM

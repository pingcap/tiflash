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
#include <VectorSearch/USearch.h>
#include <tipb/executor.pb.h>

namespace DB::DM
{

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

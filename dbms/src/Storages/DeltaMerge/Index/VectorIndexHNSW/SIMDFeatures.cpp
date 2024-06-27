// Copyright 2024 PingCAP, Inc.
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

#include <Common/Logger.h>
#include <Storages/DeltaMerge/Index/VectorIndexHNSW/SIMDFeatures.h>
#include <Storages/DeltaMerge/Index/VectorIndexHNSW/USearch.h>
#include <common/logger_useful.h>

namespace DB::DM
{

std::vector<std::string> VectorIndexHNSWSIMDFeatures::get()
{
    auto m_l2 = unum::usearch::metric_punned_t(3, unum::usearch::metric_kind_t::l2sq_k);
    auto m_cos = unum::usearch::metric_punned_t(3, unum::usearch::metric_kind_t::cos_k);
    return {
        fmt::format("hnsw.l2={}", m_l2.isa_name()),
        fmt::format("hnsw.cosine={}", m_cos.isa_name()),
    };
}

} // namespace DB::DM

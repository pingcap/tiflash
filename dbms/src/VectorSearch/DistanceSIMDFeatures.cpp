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

// SIMSIMD is header only. We don't use cmake to make these defines to avoid
// polluting all compile units.

#include <VectorSearch/DistanceSIMDFeatures.h>
#include <VectorSearch/simdsimd-internals.h>

namespace simsimd_details
{
simsimd_capability_t simd_capabilities()
{
    static simsimd_capability_t static_capabilities = simsimd_cap_any_k;
    if (static_capabilities == simsimd_cap_any_k)
        static_capabilities = simsimd_capabilities_implementation();
    return static_capabilities;
}

simsimd_capability_t actual_capability(simsimd_datatype_t data_type, simsimd_metric_kind_t kind)
{
    simsimd_metric_punned_t metric = nullptr;
    simsimd_capability_t used_capability;
    simsimd_find_metric_punned(
        kind,
        data_type,
        simsimd_details::simd_capabilities(),
        simsimd_cap_any_k,
        &metric,
        &used_capability);

    return used_capability;
}
} // namespace simsimd_details

namespace DB
{

std::vector<std::string> VectorDistanceSIMDFeatures::get()
{
    simsimd_capability_t cap_l2 = simsimd_details::actual_capability(simsimd_datatype_f32_k, simsimd_metric_l2sq_k);
    simsimd_capability_t cap_cos = simsimd_details::actual_capability(simsimd_datatype_f32_k, simsimd_metric_cos_k);

    auto cap_to_string = [](simsimd_capability_t cap) -> std::string {
        switch (cap)
        {
        case simsimd_cap_sve2_k:
            return "sve2";
        case simsimd_cap_sve_k:
            return "sve";
        case simsimd_cap_neon_k:
            return "neon";
        case simsimd_cap_skylake_k:
            return "skylake";
        case simsimd_cap_haswell_k:
            return "haswell";
        default:
            return "serial";
        }
    };

    std::vector<std::string> ret{};
    ret.push_back("vec.l2=" + cap_to_string(cap_l2));
    ret.push_back("vec.cos=" + cap_to_string(cap_cos));
    return ret;
}

} // namespace DB

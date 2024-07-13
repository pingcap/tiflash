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

#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <TiDB/Decode/Vector.h>

#include <compare>

// SIMSIMD is header only. We don't use cmake to make these defines to avoid
// polluting all compile units.

// Note: Be careful that usearch also includes simsimd with a customized config.
// Don't include simsimd and usearch at the same time. Otherwise, the effective
// config depends on the include order.
#define SIMSIMD_NATIVE_F16 0
#define SIMSIMD_NATIVE_BF16 0
#define SIMSIMD_DYNAMIC_DISPATCH 0

// Force enable all target features. We will do our own dynamic dispatch.
#define SIMSIMD_TARGET_NEON 1
#define SIMSIMD_TARGET_SVE 0 // Clang13's header does not support enableing SVE for region
#define SIMSIMD_TARGET_HASWELL 1
#define SIMSIMD_TARGET_SKYLAKE 0 // Clang13 does not support AVX512
#define SIMSIMD_TARGET_ICE 0
#define SIMSIMD_TARGET_GENOA 0
#define SIMSIMD_TARGET_SAPPHIRE 0
#include <simsimd/simsimd.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

namespace simsimd_details
{

simsimd_capability_t simd_capabilities()
{
    static simsimd_capability_t static_capabilities = simsimd_cap_any_k;
    if (static_capabilities == simsimd_cap_any_k)
        static_capabilities = simsimd_capabilities_implementation();
    return static_capabilities;
}

} // namespace simsimd_details

std::vector<std::string> VectorDistanceSIMDFeatures::get()
{
    simsimd_capability_t caps = simsimd_details::simd_capabilities();
    std::vector<std::string> ret{};
    if (caps & simsimd_cap_neon_k)
        ret.push_back("vec_distance=neon");
    if (caps & simsimd_cap_sve_k)
        ret.push_back("vec_distance=sve");
    if (caps & simsimd_cap_sve2_k)
        ret.push_back("vec_distance=sve2");
    if (caps & simsimd_cap_haswell_k)
        ret.push_back("vec_distance=haswell");
    if (caps & simsimd_cap_skylake_k)
        ret.push_back("vec_distance=skylake");
    return ret;
}

VectorFloat32Ref::VectorFloat32Ref(const Float32 * elements, size_t n)
    : elements(elements)
    , elements_n(n)
{}

void VectorFloat32Ref::checkDims(VectorFloat32Ref b) const
{
    if (size() != b.size())
        throw Exception(
            fmt::format("vectors have different dimensions: {} and {}", size(), b.size()),
            ErrorCodes::BAD_ARGUMENTS);
}

Float64 VectorFloat32Ref::l2SquaredDistance(VectorFloat32Ref b) const
{
    checkDims(b);

    static simsimd_metric_punned_t metric = nullptr;
    if (metric == nullptr)
    {
        simsimd_capability_t used_capability;
        simsimd_find_metric_punned(
            simsimd_metric_l2sq_k,
            simsimd_datatype_f32_k,
            simsimd_details::simd_capabilities(),
            simsimd_cap_any_k,
            &metric,
            &used_capability);
        if (!metric)
            return std::numeric_limits<double>::quiet_NaN();
    }

    simsimd_distance_t distance;
    metric(elements, b.elements, elements_n, &distance);

    return distance;
}

Float64 VectorFloat32Ref::innerProduct(VectorFloat32Ref b) const
{
    checkDims(b);

    static simsimd_metric_punned_t metric = nullptr;
    if (metric == nullptr)
    {
        simsimd_capability_t used_capability;
        simsimd_find_metric_punned(
            simsimd_metric_dot_k,
            simsimd_datatype_f32_k,
            simsimd_details::simd_capabilities(),
            simsimd_cap_any_k,
            &metric,
            &used_capability);
        if (!metric)
            return std::numeric_limits<double>::quiet_NaN();
    }

    simsimd_distance_t distance;
    metric(elements, b.elements, elements_n, &distance);

    return distance;
}

Float64 VectorFloat32Ref::cosineDistance(VectorFloat32Ref b) const
{
    checkDims(b);

    static simsimd_metric_punned_t metric = nullptr;
    if (metric == nullptr)
    {
        simsimd_capability_t used_capability;
        simsimd_find_metric_punned(
            simsimd_metric_cos_k,
            simsimd_datatype_f32_k,
            simsimd_details::simd_capabilities(),
            simsimd_cap_any_k,
            &metric,
            &used_capability);
        if (!metric)
            return std::numeric_limits<double>::quiet_NaN();
    }

    simsimd_distance_t distance;
    metric(elements, b.elements, elements_n, &distance);

    return distance;
}

Float64 VectorFloat32Ref::l1Distance(VectorFloat32Ref b) const
{
    checkDims(b);

    Float32 distance = 0.0;

    for (size_t i = 0, i_max = size(); i < i_max; ++i)
    {
        // Hope this can be vectorized.
        Float32 diff = std::abs(elements[i] - b[i]);
        distance += diff;
    }

    return distance;
}

Float64 VectorFloat32Ref::l2Norm() const
{
    // Note: We align the impl with pgvector: Only l2_norm use double
    // precision during calculation.

    Float64 norm = 0.0;

    for (size_t i = 0, i_max = size(); i < i_max; ++i)
    {
        // Hope this can be vectorized.
        norm += static_cast<Float64>(elements[i]) * static_cast<Float64>(elements[i]);
    }

    return std::sqrt(norm);
}

std::strong_ordering VectorFloat32Ref::operator<=>(const VectorFloat32Ref & b) const
{
    auto la = size();
    auto lb = b.size();
    auto common_len = std::min(la, lb);

    const auto * va = elements;
    const auto * vb = b.elements;

    for (size_t i = 0; i < common_len; i++)
    {
        if (va[i] < vb[i])
            return std::strong_ordering::less;
        else if (va[i] > vb[i])
            return std::strong_ordering::greater;
    }
    if (la < lb)
        return std::strong_ordering::less;
    else if (la > lb)
        return std::strong_ordering::greater;
    else
        return std::strong_ordering::equal;
}

String VectorFloat32Ref::toString() const
{
    WriteBufferFromOwnString write_buffer;
    toStringInBuffer(write_buffer);
    write_buffer.finalize();
    return write_buffer.releaseStr();
}

void VectorFloat32Ref::toStringInBuffer(WriteBuffer & write_buffer) const
{
    write_buffer.write('[');
    for (size_t i = 0; i < elements_n; i++)
    {
        if (i > 0)
        {
            write_buffer.write(',');
        }
        writeFloatText(elements[i], write_buffer);
    }
    write_buffer.write(']');
}

} // namespace DB

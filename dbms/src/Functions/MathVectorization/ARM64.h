// Copyright 2022 PingCAP, Ltd.
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
#include <arm_neon.h>

#include <cmath>
#include <cstddef>

namespace DB::MathVectorization::UnaryMath
{

#pragma push_macro("UNARY_FUNCTION_LIST")
#define UNARY_FUNCTION_LIST(M) \
    M(sin)                     \
    M(cos)                     \
    M(atan)                    \
    M(erf)                     \
    M(erfc)                    \
    M(exp)                     \
    M(log)                     \
    M(log10)

// The following functions are provided by arm-optimized-routines.
#pragma push_macro("PROTOTYPE")
#define PROTOTYPE(X) __attribute__((__aarch64_vector_pcs__)) float64x2_t __vn_##X(float64x2_t);
extern "C" {
UNARY_FUNCTION_LIST(PROTOTYPE)
}
#pragma pop_macro("PROTOTYPE")

static constexpr size_t BATCH_SIZE = 64;
static constexpr size_t VECTOR_SIZE = sizeof(float64x2_t) / sizeof(float64_t);

struct InputArray
{
    alignas(float64x2_t) float64_t data[BATCH_SIZE]{};
};

#pragma push_macro("TRANSFORM")
#define TRANSFORM(X)                                                       \
    static inline void X##TransformBatchASIMD(float64_t * __restrict dst,  \
                                              const InputArray & input)    \
    {                                                                      \
        for (size_t j = 0; j < BATCH_SIZE; j += VECTOR_SIZE)               \
        {                                                                  \
            auto vec = vld1q_f64(&input.data[j]);                          \
            auto res = __vn_##X(vec);                                      \
            vst1q_f64(&dst[j], res);                                       \
        }                                                                  \
    }                                                                      \
    static inline void X##TransformBatchScalar(float64_t * __restrict dst, \
                                               const InputArray & input)   \
    {                                                                      \
        for (size_t j = 0; j < BATCH_SIZE; ++j)                            \
        {                                                                  \
            dst[j] = ::X(input.data[j]);                                   \
        }                                                                  \
    }                                                                      \
    static inline decltype(X##TransformBatchScalar) * X##TransformBatch = &X##TransformBatchScalar;
UNARY_FUNCTION_LIST(TRANSFORM)
#pragma pop_macro("TRANSFORM")

#pragma push_macro("TRANSFORM")
#define TRANSFORM(X)                                                                     \
    template <typename T>                                                                \
    static inline void X##Transform(const T * src, double * __restrict dst, size_t size) \
    {                                                                                    \
        auto * dst_f64 = reinterpret_cast<float64_t *>(dst);                             \
        auto * batch = X##TransformBatch;                                                \
        size_t i = 0;                                                                    \
        if (batch != &X##TransformBatchScalar)                                           \
        {                                                                                \
            for (; i + BATCH_SIZE <= size; i += BATCH_SIZE)                              \
            {                                                                            \
                InputArray buffer{};                                                     \
                for (size_t j = 0; j < BATCH_SIZE; j++)                                  \
                {                                                                        \
                    buffer.data[j] = static_cast<double>(src[i + j]);                    \
                }                                                                        \
                batch(&dst_f64[i], buffer);                                              \
            }                                                                            \
        }                                                                                \
        for (; i < size; i++)                                                            \
        {                                                                                \
            dst[i] = ::X(src[i]);                                                        \
        }                                                                                \
    }

UNARY_FUNCTION_LIST(TRANSFORM)
#pragma pop_macro("TRANSFORM")

#pragma push_macro("ENABLE")
#define ENABLE(X) X##TransformBatch = &X##TransformBatchASIMD;
static inline void enableVectorizationImpl()
{
    UNARY_FUNCTION_LIST(ENABLE);
}
#pragma pop_macro("ENABLE")

#pragma push_macro("DISABLE")
#define DISABLE(X) X##TransformBatch = &X##TransformBatchScalar;
static inline void disableVectorizationImpl()
{
    UNARY_FUNCTION_LIST(DISABLE);
}
#pragma pop_macro("DISABLE")

#pragma pop_macro("UNARY_FUNCTION_LIST")
} // namespace DB::MathVectorization::UnaryMath

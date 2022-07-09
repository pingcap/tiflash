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

#include <dlfcn.h>
#include <immintrin.h>

#include <cmath>
#include <cstdint>
#include <cstring>

namespace DB::UnaryMath
{

#pragma push_macro("UNARY_FUNCTION_LIST")
#define UNARY_FUNCTION_LIST(M) \
    M(acos)                    \
    M(acosh)                   \
    M(asin)                    \
    M(asinh)                   \
    M(atan)                    \
    M(atanh)                   \
    M(cbrt)                    \
    M(cos)                     \
    M(cosh)                    \
    M(erf)                     \
    M(erfc)                    \
    M(exp)                     \
    M(exp10)                   \
    M(exp2)                    \
    M(expm1)                   \
    M(log)                     \
    M(log10)                   \
    M(log1p)                   \
    M(log2)                    \
    M(sin)                     \
    M(sinh)                    \
    M(tan)                     \
    M(tanh)

enum class LibMVecFunc
{
#pragma push_macro("ENUM_INSTANCE")
#define ENUM_INSTANCE(X) X,
    UNARY_FUNCTION_LIST(ENUM_INSTANCE)
#pragma pop_macro("ENUM_INSTANCE")
    sentinel
};

struct LibMVec
{
    void * handle;
    LibMVec()
    {
        handle = ::dlopen("libmvec.so.1", RTLD_LAZY | RTLD_LOCAL);
    }
    ~LibMVec()
    {
        ::dlclose(handle);
    }
};

template <class T, T (&Load)(const double *), void (&Store)(double *, T)>
struct MVec
{
    using UnaryFuncPtr = T (*)(T);
    using VectorType = T;
    static constexpr size_t vector_size = sizeof(T) / sizeof(double);
    static constexpr auto & load = Load;
    static constexpr auto & store = Store;

    UnaryFuncPtr funcs[static_cast<size_t>(LibMVecFunc::sentinel)]{};

    MVec(const char * prefix, void * handle)
    {
        char sym_buffer[64]{};
        auto length = strlen(prefix);
        std::memcpy(sym_buffer, prefix, length);
#pragma push_macro("LOAD")
#define LOAD(X)                                                                                                       \
    {                                                                                                                 \
        std::strcpy(&sym_buffer[length], #X);                                                                         \
        if (handle)                                                                                                   \
        {                                                                                                             \
            funcs[static_cast<size_t>(LibMVecFunc::X)] = reinterpret_cast<UnaryFuncPtr>(::dlsym(handle, sym_buffer)); \
        }                                                                                                             \
    }
        UNARY_FUNCTION_LIST(LOAD);
#pragma pop_macro("LOAD")
    }
};

static inline __attribute__((target("avx512f"))) __m512d load512(const double * data)
{
    return _mm512_load_pd(data);
}

static inline __attribute__((target("avx512f"))) void store512(double * data, __m512d vec)
{
    _mm512_store_pd(data, vec);
}

static inline const LibMVec LIBMVEC_LIBRARY{};
static inline const MVec<__m128d, _mm_load_pd, _mm_store_pd> LIBMDEV_SSE2_FUNCTIONS{"_ZGVbN2v_", LIBMVEC_LIBRARY.handle};
static inline const MVec<__m256d, _mm256_load_pd, _mm256_store_pd> LIBMDEV_AVX2_FUNCTIONS{"_ZGVdN4v_", LIBMVEC_LIBRARY.handle};
static inline const MVec<__m512d, load512, store512> LIBMDEV_AVX512_FUNCTIONS{"_ZGVeN8v_", LIBMVEC_LIBRARY.handle};

static constexpr size_t BATCH_SIZE = 64;
struct InputArray
{
    alignas(64) double data[BATCH_SIZE];
};
using InputArrayRef = const InputArray &;

#pragma push_macro("TRANSFORM")
#define TRANSFORM(X, ATTR, VARIANT)                                                                                            \
    __attribute__((target(#ATTR))) static inline void X##TransformBatch##VARIANT(double * __restrict dst, InputArrayRef input) \
    {                                                                                                                          \
        using ImplType = decltype(LIBMDEV_##VARIANT##_FUNCTIONS);                                                              \
        auto function = LIBMDEV_##VARIANT##_FUNCTIONS.funcs[static_cast<size_t>(LibMVecFunc::X)];                              \
        for (size_t i = 0; i < BATCH_SIZE; i += ImplType::vector_size)                                                         \
        {                                                                                                                      \
            ImplType::VectorType data = ImplType::load(&input.data[i]);                                                        \
            ImplType::VectorType result = function(data);                                                                      \
            ImplType::store(&dst[i], result);                                                                                  \
        }                                                                                                                      \
    }

#pragma push_macro("TRANSFORM_AVX512")
#define TRANSFORM_AVX512(X) TRANSFORM(X, avx512f, AVX512)
UNARY_FUNCTION_LIST(TRANSFORM_AVX512)
#pragma pop_macro("TRANSFORM_AVX512")

#pragma push_macro("TRANSFORM_AVX2")
#define TRANSFORM_AVX2(X) TRANSFORM(X, avx2, AVX2)
UNARY_FUNCTION_LIST(TRANSFORM_AVX2)
#pragma pop_macro("TRANSFORM_AVX2")

#pragma push_macro("TRANSFORM_SSE2")
#define TRANSFORM_SSE2(X) TRANSFORM(X, sse2, SSE2)
UNARY_FUNCTION_LIST(TRANSFORM_SSE2)
#pragma pop_macro("TRANSFORM_SSE2")
#pragma pop_macro("TRANSFORM")

#pragma push_macro("SELECT")
#define SELECT(X)                                                                                                     \
    static inline void X##TransformBatchDefault(double * __restrict dst, InputArrayRef input)                         \
    {                                                                                                                 \
        for (size_t i = 0; i < BATCH_SIZE; ++i)                                                                       \
        {                                                                                                             \
            dst[i] = ::X(input.data[i]);                                                                              \
        }                                                                                                             \
    }                                                                                                                 \
    static inline decltype(X##TransformBatchDefault) & X##TransformBatchSelect()                                      \
    {                                                                                                                 \
        if (__builtin_cpu_supports("avx512f") && LIBMDEV_AVX512_FUNCTIONS.funcs[static_cast<size_t>(LibMVecFunc::X)]) \
        {                                                                                                             \
            return X##TransformBatch##AVX512;                                                                         \
        }                                                                                                             \
        if (__builtin_cpu_supports("avx2") && LIBMDEV_AVX2_FUNCTIONS.funcs[static_cast<size_t>(LibMVecFunc::X)])      \
        {                                                                                                             \
            return X##TransformBatch##AVX2;                                                                           \
        }                                                                                                             \
        if (__builtin_cpu_supports("sse2") && LIBMDEV_SSE2_FUNCTIONS.funcs[static_cast<size_t>(LibMVecFunc::X)])      \
        {                                                                                                             \
            return X##TransformBatch##SSE2;                                                                           \
        }                                                                                                             \
        return X##TransformBatchDefault;                                                                              \
    }                                                                                                                 \
    static inline decltype(X##TransformBatchDefault) & X##TransformBatch = X##TransformBatchSelect();
UNARY_FUNCTION_LIST(SELECT)
#pragma pop_macro("SELECT")

#pragma push_macro("TRANSFORM")
#define TRANSFORM(X)                                                                                \
    template <typename T>                                                                           \
    static inline void X##Transform(double * __restrict dst, const T * __restrict src, size_t size) \
    {                                                                                               \
        InputArray input_data;                                                                      \
        auto address = reinterpret_cast<uintptr_t>(dst);                                            \
        auto remainder = address % BATCH_SIZE;                                                      \
        auto offset = (BATCH_SIZE - (remainder == 0 ? BATCH_SIZE : remainder)) / sizeof(double);    \
        size_t i = 0;                                                                               \
        for (; i < offset; ++i)                                                                     \
        {                                                                                           \
            dst[i] = ::X(static_cast<double>(src[i]));                                              \
        }                                                                                           \
        for (; i + BATCH_SIZE <= size; i += BATCH_SIZE)                                             \
        {                                                                                           \
            for (size_t j = 0; j < BATCH_SIZE; ++j)                                                 \
            {                                                                                       \
                input_data.data[j] = static_cast<double>(src[i + j]);                               \
            }                                                                                       \
            X##TransformBatch(&dst[i], input_data);                                                 \
        }                                                                                           \
        for (; i < size; ++i)                                                                       \
        {                                                                                           \
            dst[i] = ::X(static_cast<double>(src[i]));                                              \
        }                                                                                           \
    }
UNARY_FUNCTION_LIST(TRANSFORM)
#pragma pop_macro("TRANSFORM")

#pragma pop_macro("UNARY_FUNCTION_LIST")
} // namespace DB::UnaryMath

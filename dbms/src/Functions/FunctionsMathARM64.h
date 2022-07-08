#include <arm_neon.h>

#include <cmath>
#include <cstddef>

namespace DB::UnaryMath
{

#pragma push_macro("__vpcs")
#define __vpcs __attribute__((__aarch64_vector_pcs__))
extern "C" {
__vpcs float64x2_t __vn_sin(float64x2_t);
__vpcs float64x2_t __vn_cos(float64x2_t);
__vpcs float64x2_t __vn_exp(float64x2_t);
__vpcs float64x2_t __vn_log(float64x2_t);
}
#pragma pop_macro("__vpcs")

static constexpr size_t BATCH_SIZE = 64;
static constexpr size_t VECTOR_SIZE = sizeof(float64x2_t) / sizeof(float64_t);

#pragma push_macro("TRANSFORM")
#define TRANSFORM(X)                                                                     \
    template <typename T>                                                                \
    static inline void X##Transform(double * __restrict dst, const T * src, size_t size) \
    {                                                                                    \
        auto * dst_f64 = reinterpret_cast<float64_t *>(dst);                             \
        size_t i = 0;                                                                    \
        for (; i + BATCH_SIZE <= size; i += BATCH_SIZE)                                  \
        {                                                                                \
            alignas(float64x2_t) float64_t buffer[BATCH_SIZE]{};                         \
            for (size_t j = 0; j < BATCH_SIZE; j++)                                      \
            {                                                                            \
                buffer[j] = static_cast<double>(src[i + j]);                             \
            }                                                                            \
            for (size_t j = 0; j < BATCH_SIZE; j += VECTOR_SIZE)                         \
            {                                                                            \
                auto vec = vld1q_f64(&buffer[j]);                                        \
                auto res = __vn_##X(vec);                                                \
                vst1q_f64(&dst_f64[i + j], res);                                         \
            }                                                                            \
        }                                                                                \
        for (; i < size; i++)                                                            \
        {                                                                                \
            dst[i] = ::X(src[i]);                                                        \
        }                                                                                \
    }

TRANSFORM(sin)
TRANSFORM(cos)
TRANSFORM(exp)
TRANSFORM(log)
#pragma pop_macro("TRANSFORM")
} // namespace DB::UnaryMath

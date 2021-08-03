#ifdef DBMS_ENABLE_AVX512_SUPPORT
#define LIBDIVIDE_AVX512
#include <Functions/FunctionsArithmetic.h>
#include <immintrin.h>
#include <libdivide.h>

namespace DB
{
template <typename A, typename B>
void vectorizedDivisionLoopAVX512(typename DivideIntegralImpl<A, B>::ResultType *& c_pos, A *& a_pos, B b, size_t size)
{
    using VectorType = __m512i;
    libdivide::divider<A> divider(b);
    static constexpr size_t values_per_sse_register = sizeof(VectorType) / sizeof(A);
    const A * a_end_sse = a_pos + size / values_per_sse_register * values_per_sse_register;

    while (a_pos < a_end_sse)
    {
        _mm512_storeu_si512(
            reinterpret_cast<VectorType *>(c_pos), _mm512_loadu_si512(reinterpret_cast<const VectorType *>(a_pos)) / divider);

        a_pos += values_per_sse_register;
        c_pos += values_per_sse_register;
    }
}

#define VECTORIZED_DIV_LOOP_AVX512_INSTANTIATION(A, B) \
    template void vectorizedDivisionLoopAVX512<A, B>(typename DivideIntegralImpl<A, B>::ResultType * &c_pos, A * &a_pos, B b, size_t size);

VECTORIZED_DIV_LOOP_AVX512_INSTANTIATION(UInt64, UInt64)
VECTORIZED_DIV_LOOP_AVX512_INSTANTIATION(UInt64, UInt32)
VECTORIZED_DIV_LOOP_AVX512_INSTANTIATION(UInt64, UInt16)
VECTORIZED_DIV_LOOP_AVX512_INSTANTIATION(UInt64, UInt8)

VECTORIZED_DIV_LOOP_AVX512_INSTANTIATION(UInt32, UInt64)
VECTORIZED_DIV_LOOP_AVX512_INSTANTIATION(UInt32, UInt32)
VECTORIZED_DIV_LOOP_AVX512_INSTANTIATION(UInt32, UInt16)
VECTORIZED_DIV_LOOP_AVX512_INSTANTIATION(UInt32, UInt8)

} //namespace DB
#endif
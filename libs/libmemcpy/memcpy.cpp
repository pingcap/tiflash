#include "memcpy.h"

#include <immintrin.h>

#ifndef TIFLASH_MEMCPY_PREFIX
#define TIFLASH_MEMCPY_PREFIX
#endif
#define TIFLASH_MEMCPY TIFLASH_MACRO_CONCAT(TIFLASH_MEMCPY_PREFIX, memcpy)
/// This is needed to generate an object file for linking.

extern "C" __attribute__((visibility("default"))) void * TIFLASH_MEMCPY(void * __restrict dst, const void * __restrict src, size_t size) noexcept
{
    return inline_memcpy(dst, src, size);
}

namespace memory_copy
{
MemcpyConfig memcpy_config = {
    .medium_size_threshold = 0x800,
    .huge_size_threshold = 0xc0000,
    .page_size = 0x1000,
    .medium_size_strategy = MediumSizeStrategy::MediumSizeSSE,
    .huge_size_strategy = HugeSizeStrategy::HugeSizeSSE};

namespace detail
{
template <uint8_t delta>
__attribute__((always_inline, target("ssse3"))) static inline void memcpy_ssse3_loop(
    char * __restrict & __restrict dst,
    char const * __restrict & __restrict src,
    size_t & size)
{
    src -= delta;
    size += delta;

    __m128i cell[9];
    cell[8] = _mm_load_si128(reinterpret_cast<const __m128i *>(src));
    while (size >= 144)
    {
        const auto * source = reinterpret_cast<const __m128i *>(__builtin_assume_aligned(src, sizeof(__m128i)));
        auto * target = reinterpret_cast<__m128i *>(__builtin_assume_aligned(dst, sizeof(__m128i)));

        __builtin_prefetch(src + 9 * 16);

        cell[0] = cell[8];
        TIFLASH_MEMCPY_UNROLL_FULLY
        for (size_t i = 1; i < std::size(cell); ++i)
        {
            cell[i] = _mm_load_si128(source + i);
        }

        src += 128;

        if constexpr (delta != 0)
        {
            TIFLASH_MEMCPY_UNROLL_FULLY
            for (size_t i = 0; i < std::size(cell) - 1; ++i)
            {
                cell[i] = _mm_alignr_epi8(cell[i + 1], cell[i], delta);
            }
        }

        TIFLASH_MEMCPY_UNROLL_FULLY
        for (size_t i = 0; i < std::size(cell) - 1; ++i)
        {
            _mm_stream_si128(target + i, cell[i]);
        }
        dst += 128;
        size -= 128;
    }

    if (size >= 128) // 128 <= size <= 144, load 8 registers and store 7 registers
    {
        auto limit = 128 / sizeof(__m128i); // 8 registers
        const auto * source = reinterpret_cast<const __m128i *>(__builtin_assume_aligned(src, sizeof(__m128i)));
        auto * target = reinterpret_cast<__m128i *>(__builtin_assume_aligned(dst, sizeof(__m128i)));

        TIFLASH_MEMCPY_UNROLL_FULLY
        for (size_t i = 0; i < limit; ++i)
        {
            cell[i] = _mm_load_si128(source + i);
        }

        if constexpr (delta != 0)
        {
            TIFLASH_MEMCPY_UNROLL_FULLY
            for (size_t i = 0; i < limit - 1; ++i)
            {
                cell[i] = _mm_alignr_epi8(cell[i + 1], cell[i], delta);
            }
        }

        TIFLASH_MEMCPY_UNROLL_FULLY
        for (size_t i = 0; i < limit - 1; ++i)
        {
            _mm_stream_si128(target + i, cell[i]);
        }

        src += (limit - 1) * sizeof(__m128i);
        dst += (limit - 1) * sizeof(__m128i);
        size -= (limit - 1) * sizeof(__m128i);
    }

    dst -= delta;
}

__attribute__((target("ssse3"))) static inline void memcpy_ssse3_mux(
    char * __restrict & __restrict dst,
    char const * __restrict & __restrict src,
    size_t & size)
{
    auto dst_padding = (-reinterpret_cast<uintptr_t>(dst)) & (sizeof(__m128i) - 1);
    tiflash_compiler_builtin_memcpy(dst, src, 16);
    dst += dst_padding;
    src += dst_padding;
    size -= dst_padding;

    auto delta = reinterpret_cast<uintptr_t>(src) & 15;

    switch (delta)
    {
    case 0:
        memcpy_ssse3_loop<0>(dst, src, size);
        break;
    case 1:
        memcpy_ssse3_loop<1>(dst, src, size);
        break;
    case 2:
        memcpy_ssse3_loop<2>(dst, src, size);
        break;
    case 3:
        memcpy_ssse3_loop<3>(dst, src, size);
        break;
    case 4:
        memcpy_ssse3_loop<4>(dst, src, size);
        break;
    case 5:
        memcpy_ssse3_loop<5>(dst, src, size);
        break;
    case 6:
        memcpy_ssse3_loop<6>(dst, src, size);
        break;
    case 7:
        memcpy_ssse3_loop<7>(dst, src, size);
        break;
    case 8:
        memcpy_ssse3_loop<8>(dst, src, size);
        break;
    case 9:
        memcpy_ssse3_loop<9>(dst, src, size);
        break;
    case 10:
        memcpy_ssse3_loop<10>(dst, src, size);
        break;
    case 11:
        memcpy_ssse3_loop<11>(dst, src, size);
        break;
    case 12:
        memcpy_ssse3_loop<12>(dst, src, size);
        break;
    case 13:
        memcpy_ssse3_loop<13>(dst, src, size);
        break;
    case 14:
        memcpy_ssse3_loop<14>(dst, src, size);
        break;
    default:
        memcpy_ssse3_loop<15>(dst, src, size);
        break;
    }
}

template <typename Vector, size_t page_num, size_t vec_num, typename Load, typename Store>
__attribute__((always_inline, target("avx512vl"))) static inline void memcpy_evex_impl(
    char * __restrict & __restrict dst,
    char const * __restrict & __restrict src,
    size_t & __restrict size,
    Load load,
    Store store)
{
    constexpr size_t stride_size = vec_num * sizeof(Vector);
    const auto page_size = memcpy_config.page_size;
    Vector storage[vec_num * page_num];

    while (size >= page_num * page_size)
    {
        for (size_t i = 0; i < page_size / stride_size; ++i)
        {
            auto * target = static_cast<char *>(__builtin_assume_aligned(dst, alignof(Vector)));
            const auto * source = static_cast<const char *>(src);

            // prefetch one stride per-page
            TIFLASH_MEMCPY_UNROLL_FULLY
            for (size_t p = 0; p < page_num; ++p)
            {
                __builtin_prefetch(source + page_size * p + stride_size);
            };

            TIFLASH_MEMCPY_UNROLL_FULLY
            for (size_t p = 0; p < page_num; ++p)
            {
                TIFLASH_MEMCPY_UNROLL_FULLY
                for (size_t v = 0; v < vec_num; ++v)
                {
                    const auto * address = reinterpret_cast<const Vector *>(source + page_size * p + sizeof(Vector) * v);
                    storage[p * vec_num + v] = load(address);
                };
            };

            TIFLASH_MEMCPY_UNROLL_FULLY
            for (size_t p = 0; p < page_num; ++p)
            {
                TIFLASH_MEMCPY_UNROLL_FULLY
                for (size_t v = 0; v < vec_num; ++v)
                {
                    auto * address = reinterpret_cast<Vector *>(target + page_size * p + sizeof(Vector) * v);
                    store(address, storage[p * vec_num + v]);
                }
            }
            dst += stride_size;
            src += stride_size;
        }
        dst += (page_num - 1) * page_size;
        src += (page_num - 1) * page_size;
        size -= page_num * page_size;
    }
}

template <typename Vector, size_t page_num, size_t vec_num, typename Load, typename Store>
__attribute__((always_inline, target("avx2"))) static inline void memcpy_vex_impl(
    char * __restrict & __restrict dst,
    char const * __restrict & __restrict src,
    size_t & __restrict size,
    Load load,
    Store store)
{
    constexpr size_t stride_size = vec_num * sizeof(Vector);
    const auto page_size = memcpy_config.page_size;
    Vector storage[vec_num * page_num]{};

    while (size >= page_num * page_size)
    {
        for (size_t i = 0; i < page_size / stride_size; ++i)
        {
            auto * target = static_cast<char *>(__builtin_assume_aligned(dst, alignof(Vector)));
            const auto * source = static_cast<const char *>(src);

            // prefetch one stride per-page
            TIFLASH_MEMCPY_UNROLL_FULLY
            for (size_t p = 0; p < page_num; ++p)
            {
                __builtin_prefetch(source + page_size * p + stride_size);
            };

            TIFLASH_MEMCPY_UNROLL_FULLY
            for (size_t p = 0; p < page_num; ++p)
            {
                TIFLASH_MEMCPY_UNROLL_FULLY
                for (size_t v = 0; v < vec_num; ++v)
                {
                    const auto * address = reinterpret_cast<const Vector *>(source + page_size * p + sizeof(Vector) * v);
                    storage[p * vec_num + v] = load(address);
                };
            };

            TIFLASH_MEMCPY_UNROLL_FULLY
            for (size_t p = 0; p < page_num; ++p)
            {
                TIFLASH_MEMCPY_UNROLL_FULLY
                for (size_t v = 0; v < vec_num; ++v)
                {
                    auto * address = reinterpret_cast<Vector *>(target + page_size * p + sizeof(Vector) * v);
                    store(address, storage[p * vec_num + v]);
                }
            }
            dst += stride_size;
            src += stride_size;
        }
        dst += (page_num - 1) * page_size;
        src += (page_num - 1) * page_size;
        size -= page_num * page_size;
    }
}

__attribute__((target("avx2"))) static inline void memcpy_vex32(
    char * __restrict & __restrict dst,
    char const * __restrict & __restrict src,
    size_t & size)
{
    auto dst_padding = (-reinterpret_cast<uintptr_t>(dst)) & (sizeof(__m256i) - 1);
    auto diff = (reinterpret_cast<uintptr_t>(dst) ^ reinterpret_cast<uintptr_t>(src)) & (sizeof(__m256i) - 1);
    tiflash_compiler_builtin_memcpy(dst, src, sizeof(__m256i));
    dst += dst_padding;
    src += dst_padding;
    size -= dst_padding;
    if (size >= 16 * memcpy_config.huge_size_threshold)
    {
        if (diff == 0)
        {
            memcpy_vex_impl<__m256i, 4, 4>(dst, src, size, _mm256_load_si256, _mm256_stream_si256);
        }
        else
        {
            memcpy_vex_impl<__m256i, 4, 4>(dst, src, size, _mm256_loadu_si256, _mm256_stream_si256);
        }
    }
    else
    {
        if (diff == 0)
        {
            memcpy_vex_impl<__m256i, 2, 4>(dst, src, size, _mm256_load_si256, _mm256_stream_si256);
        }
        else
        {
            memcpy_vex_impl<__m256i, 2, 4>(dst, src, size, _mm256_loadu_si256, _mm256_stream_si256);
        }
    }
    memcpy_sse_loop<false, false>(dst, src, size);
}

__attribute__((target("avx512f,avx512vl"))) static inline void memcpy_evex32(
    char * __restrict & __restrict dst,
    char const * __restrict & __restrict src,
    size_t & size)
{
    auto dst_padding = (-reinterpret_cast<uintptr_t>(dst)) & (sizeof(__m256i) - 1);
    auto diff = (reinterpret_cast<uintptr_t>(dst) ^ reinterpret_cast<uintptr_t>(src)) & (sizeof(__m256i) - 1);
    tiflash_compiler_builtin_memcpy(dst, src, sizeof(__m256i));
    dst += dst_padding;
    src += dst_padding;
    size -= dst_padding;
    if (size >= 16 * memcpy_config.huge_size_threshold)
    {
        if (diff == 0)
        {
            memcpy_evex_impl<__m256i, 4, 4>(dst, src, size, _mm256_load_si256, _mm256_stream_si256);
        }
        else
        {
            memcpy_evex_impl<__m256i, 4, 4>(dst, src, size, _mm256_loadu_si256, _mm256_stream_si256);
        }
    }
    else
    {
        if (diff == 0)
        {
            memcpy_evex_impl<__m256i, 2, 4>(dst, src, size, _mm256_load_si256, _mm256_stream_si256);
        }
        else
        {
            memcpy_evex_impl<__m256i, 2, 4>(dst, src, size, _mm256_loadu_si256, _mm256_stream_si256);
        }
    }
    memcpy_sse_loop<false, false>(dst, src, size);
}

__attribute__((target("avx512f,avx512vl"))) static inline void memcpy_evex64(
    char * __restrict & __restrict dst,
    char const * __restrict & __restrict src,
    size_t & size)
{
    auto dst_padding = (-reinterpret_cast<uintptr_t>(dst)) & (sizeof(__m512i) - 1);
    auto diff = (reinterpret_cast<uintptr_t>(dst) ^ reinterpret_cast<uintptr_t>(src)) & (sizeof(__m512i) - 1);
    tiflash_compiler_builtin_memcpy(dst, src, sizeof(__m512i));
    dst += dst_padding;
    src += dst_padding;
    size -= dst_padding;
    if (size >= 16 * memcpy_config.huge_size_threshold)
    {
        if (diff == 0)
        {
            memcpy_evex_impl<__m512i, 4, 4>(dst, src, size, _mm512_load_si512, _mm512_stream_si512);
        }
        else
        {
            memcpy_evex_impl<__m512i, 4, 4>(dst, src, size, _mm512_loadu_si512, _mm512_stream_si512);
        }
    }
    else
    {
        if (diff == 0)
        {
            memcpy_evex_impl<__m512i, 2, 4>(dst, src, size, _mm512_load_si512, _mm512_stream_si512);
        }
        else
        {
            memcpy_evex_impl<__m512i, 2, 4>(dst, src, size, _mm512_loadu_si512, _mm512_stream_si512);
        }
    }
    memcpy_sse_loop<false, false>(dst, src, size);
}

bool memcpy_large(
    char * __restrict & __restrict dst,
    char const * __restrict & __restrict src,
    size_t & size)
{
    if (size < memcpy_config.huge_size_threshold)
    {
        // medium sizes
        switch (memcpy_config.medium_size_strategy)
        {
        case MediumSizeStrategy::MediumSizeRepMovsb:
            detail::rep_movsb(dst, src, size);
            return true;
        default:
            detail::memcpy_sse_loop<false, false>(dst, src, size);
        }
    }
    else
    {
        // huge sizes
        switch (memcpy_config.huge_size_strategy)
        {
        case HugeSizeStrategy::HugeSizeRepMovsb:
            detail::rep_movsb(dst, src, size);
            return true;
        case HugeSizeStrategy::HugeSizeSSSE3Mux:
            detail::memcpy_ssse3_mux(dst, src, size);
            break;
        case HugeSizeStrategy::HugeSizeEVEX32:
            detail::memcpy_evex32(dst, src, size);
            break;
        case HugeSizeStrategy::HugeSizeEVEX64:
            detail::memcpy_evex64(dst, src, size);
            break;
        case HugeSizeStrategy::HugeSizeVEX32:
            detail::memcpy_vex32(dst, src, size);
            break;
        case HugeSizeStrategy::HugeSizeSSENT:
            detail::memcpy_sse_loop<true, true>(dst, src, size);
            break;
        default:
            detail::memcpy_sse_loop<false, false>(dst, src, size);
        }
    }
    return false;
}

} // namespace detail
} // namespace memory_copy

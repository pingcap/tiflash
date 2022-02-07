#include <benchmark/benchmark.h>

#include <random>
#include <vector>
#define TIFLASH_MEMCPY_PREFIX tiflash_
#include "../memcpy.cpp"

using namespace memory_copy;

struct Guard
{
    MemcpyConfig config;
    Guard()
    {
        config = memcpy_config;
    }

    ~Guard()
    {
        memcpy_config = config;
    }
};

static size_t SEED = std::random_device{}();

template <class Copy>
static inline void memcpy_bench(
    benchmark::State & state,
    Copy copy,
    size_t lower,
    size_t upper,
    size_t instance,
    MediumSizeStrategy medium = memory_copy::MediumSizeStrategy::MediumSizeSSE,
    HugeSizeStrategy huge = memory_copy::HugeSizeStrategy::HugeSizeSSE)
{
    Guard guard;
    std::vector<std::pair<std::vector<char>, std::vector<char>>> test_cases(instance);
    std::default_random_engine eng(SEED);
    std::uniform_int_distribution<size_t> dist(lower, upper);
    memcpy_config.medium_size_threshold = 2048;
    memcpy_config.huge_size_threshold = 0xc0000;
    if (!check_valid_strategy(medium) || !check_valid_strategy(huge))
    {
        state.SkipWithError("strategy is not available on this target");
    }
    memcpy_config.medium_size_strategy = medium;
    memcpy_config.huge_size_strategy = huge;
    size_t sum = 0;
    for (auto & i : test_cases)
    {
        auto size = dist(eng);
        i.first.resize(size);
        i.second.resize(size);
        sum += size;
    }
    for (auto _ : state)
    {
        for (auto & i : test_cases)
        {
            copy(i.second.data(), i.first.data(), i.first.size());
        }
    }
    state.SetBytesProcessed(sum * state.iterations());
}

extern "C" NO_INLINE void * legacy_memcpy(void * __restrict dst_, const void * __restrict src_, size_t size) noexcept
{
    /// We will use pointer arithmetic, so char pointer will be used.
    /// Note that __restrict makes sense (otherwise compiler will reload data from memory
    /// instead of using the value of registers due to possible aliasing).
    char * __restrict dst = reinterpret_cast<char * __restrict>(dst_);
    const char * __restrict src = reinterpret_cast<const char * __restrict>(src_);

    /// Standard memcpy returns the original value of dst. It is rarely used but we have to do it.
    /// If you use memcpy with small but non-constant sizes, you can call inline_memcpy directly
    /// for inlining and removing this single instruction.
    void * ret = dst;

tail:
    /// Small sizes and tails after the loop for large sizes.
    /// The order of branches is important but in fact the optimal order depends on the distribution of sizes in your application.
    /// This order of branches is from the disassembly of glibc's code.
    /// We copy chunks of possibly uneven size with two overlapping movs.
    /// Example: to copy 5 bytes [0, 1, 2, 3, 4] we will copy tail [1, 2, 3, 4] first and then head [0, 1, 2, 3].
    if (size <= 16)
    {
        if (size >= 8)
        {
            /// Chunks of 8..16 bytes.
            tiflash_compiler_builtin_memcpy(dst + size - 8, src + size - 8, 8);
            tiflash_compiler_builtin_memcpy(dst, src, 8);
        }
        else if (size >= 4)
        {
            /// Chunks of 4..7 bytes.
            tiflash_compiler_builtin_memcpy(dst + size - 4, src + size - 4, 4);
            tiflash_compiler_builtin_memcpy(dst, src, 4);
        }
        else if (size >= 2)
        {
            /// Chunks of 2..3 bytes.
            tiflash_compiler_builtin_memcpy(dst + size - 2, src + size - 2, 2);
            tiflash_compiler_builtin_memcpy(dst, src, 2);
        }
        else if (size >= 1)
        {
            /// A single byte.
            *dst = *src;
        }
        /// No bytes remaining.
    }
    else
    {
        /// Medium and large sizes.
        if (size <= 128)
        {
            /// Medium size, not enough for full loop unrolling.

            /// We will copy the last 16 bytes.
            tiflash_compiler_builtin_memcpy(dst + size - 16, src + size - 16, 16);

            /// Then we will copy every 16 bytes from the beginning in a loop.
            /// The last loop iteration will possibly overwrite some part of already copied last 16 bytes.
            /// This is Ok, similar to the code for small sizes above.
            while (size > 16)
            {
                tiflash_compiler_builtin_memcpy(dst, src, 16);
                dst += 16;
                src += 16;
                size -= 16;
            }
        }
        else
        {
            /// Large size with fully unrolled loop.

            /// Align destination to 16 bytes boundary.
            size_t padding = (16 - (reinterpret_cast<size_t>(dst) & 15)) & 15;

            /// If not aligned - we will copy first 16 bytes with unaligned stores.
            if (padding > 0)
            {
                tiflash_compiler_builtin_memcpy(dst, src, 16);
                dst += padding;
                src += padding;
                size -= padding;
            }

            /// Aligned unrolled copy. We will use half of available SSE registers.
            /// It's not possible to have both src and dst aligned.
            /// So, we will use aligned stores and unaligned loads.
            __m128i c0, c1, c2, c3, c4, c5, c6, c7;

            while (size >= 128)
            {
                c0 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 0);
                c1 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 1);
                c2 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 2);
                c3 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 3);
                c4 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 4);
                c5 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 5);
                c6 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 6);
                c7 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 7);
                src += 128;
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 0), c0);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 1), c1);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 2), c2);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 3), c3);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 4), c4);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 5), c5);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 6), c6);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 7), c7);
                dst += 128;

                size -= 128;
            }

            /// The latest remaining 0..127 bytes will be processed as usual.
            goto tail;
        }
    }
    return ret;
}




BENCHMARK_CAPTURE(memcpy_bench, system_1_to_0xc0000, ::memcpy, 1, 0xc0000, 256);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_1_to_0xc0000_sse, ::tiflash_memcpy, 1, 0xc0000, 256, MediumSizeStrategy::MediumSizeSSE);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_1_to_0xc0000_rep_movsb, ::tiflash_memcpy, 1, 0xc0000, 256, MediumSizeStrategy::MediumSizeRepMovsb);
BENCHMARK_CAPTURE(memcpy_bench, legacy_1_to_0xc0000, ::legacy_memcpy, 1, 0xc0000, 256);

BENCHMARK_CAPTURE(memcpy_bench, system_0xc0000_to_0x1200000, ::memcpy, 0xc0000, 0x1200000, 64);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_0xc0000_to_0x1200000_sse, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeSSE);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_0xc0000_to_0x1200000_ssent, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeSSENT);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_0xc0000_to_0x1200000_ssse3, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeSSSE3Mux);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_0xc0000_to_0x1200000_rep_movsb, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeRepMovsb);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_0xc0000_to_0x1200000_vex32, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeVEX32);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_0xc0000_to_0x1200000_evex32, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeEVEX32);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_0xc0000_to_0x1200000_evex32, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeEVEX64);
BENCHMARK_CAPTURE(memcpy_bench, legacy_0xc0000_to_0x1200000, ::legacy_memcpy, 0xc0000, 0x1200000, 64);

BENCHMARK_CAPTURE(memcpy_bench, system_random, ::memcpy, 0xc0000, 0x1200000, 64);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_sse_sse, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeSSE);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_sse_ssent, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeSSENT);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_sse_ssse3, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeSSSE3Mux);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_sse_rep_movsb, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeRepMovsb);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_sse_vex32, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeVEX32);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_sse_evex32, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeEVEX32);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_sse_evex64, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeSSE, HugeSizeStrategy::HugeSizeEVEX32);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_rep_movsb_sse, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeRepMovsb, HugeSizeStrategy::HugeSizeSSE);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_rep_movsb_ssent, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeRepMovsb, HugeSizeStrategy::HugeSizeSSENT);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_rep_movsb_ssse3, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeRepMovsb, HugeSizeStrategy::HugeSizeSSSE3Mux);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_rep_movsb_rep_movsb, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeRepMovsb, HugeSizeStrategy::HugeSizeRepMovsb);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_rep_movsb_vex32, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeRepMovsb, HugeSizeStrategy::HugeSizeVEX32);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_rep_movsb_evex32, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeRepMovsb, HugeSizeStrategy::HugeSizeEVEX32);
BENCHMARK_CAPTURE(memcpy_bench, tiflash_random_rep_movsb_evex64, ::tiflash_memcpy, 0xc0000, 0x1200000, 64, MediumSizeStrategy::MediumSizeRepMovsb, HugeSizeStrategy::HugeSizeEVEX64);
BENCHMARK_CAPTURE(memcpy_bench, legacy_random, ::legacy_memcpy, 0xc0000, 0x1200000, 64);

BENCHMARK_MAIN();
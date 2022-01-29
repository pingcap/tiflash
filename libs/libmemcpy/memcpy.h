#include <immintrin.h>

#include <cstddef>


/** Custom memcpy implementation for ClickHouse.
  * It has the following benefits over using glibc's implementation:
  * 1. Avoiding dependency on specific version of glibc's symbol, like memcpy@@GLIBC_2.14 for portability.
  * 2. Avoiding indirect call via PLT due to shared linking, that can be less efficient.
  * 3. It's possible to include this header and call inline_memcpy directly for better inlining or interprocedural analysis.
  * 4. Better results on our performance tests on current CPUs: up to 25% on some queries and up to 0.7%..1% in average across all queries.
  *
  * Writing our own memcpy is extremely difficult for the following reasons:
  * 1. The optimal variant depends on the specific CPU model.
  * 2. The optimal variant depends on the distribution of size arguments.
  * 3. It depends on the number of threads copying data concurrently.
  * 4. It also depends on how the calling code is using the copied data and how the different memcpy calls are related to each other.
  * Due to vast range of scenarios it makes proper testing especially difficult.
  * When writing our own memcpy there is a risk to overoptimize it
  * on non-representative microbenchmarks while making real-world use cases actually worse.
  *
  * Most of the benchmarks for memcpy on the internet are wrong.
  *
  * Let's look at the details:
  *
  * For small size, the order of branches in code is important.
  * There are variants with specific order of branches (like here or in glibc)
  * or with jump table (in asm code see example from Cosmopolitan libc:
  * https://github.com/jart/cosmopolitan/blob/de09bec215675e9b0beb722df89c6f794da74f3f/libc/nexgen32e/memcpy.S#L61)
  * or with Duff device in C (see https://github.com/skywind3000/FastMemcpy/)
  *
  * It's also important how to copy uneven sizes.
  * Almost every implementation, including this, is using two overlapping movs.
  *
  * It is important to disable -ftree-loop-distribute-patterns when compiling memcpy implementation,
  * otherwise the compiler can replace internal loops to a call to memcpy that will lead to infinite recursion.
  *
  * For larger sizes it's important to choose the instructions used:
  * - SSE or AVX or AVX-512;
  * - rep movsb;
  * Performance will depend on the size threshold, on the CPU model, on the "erms" flag
  * ("Enhansed Rep MovS" - it indicates that performance of "rep movsb" is decent for large sizes)
  * https://stackoverflow.com/questions/43343231/enhanced-rep-movsb-for-memcpy
  *
  * Using AVX-512 can be bad due to throttling.
  * Using AVX can be bad if most code is using SSE due to switching penalty
  * (it also depends on the usage of "vzeroupper" instruction).
  * But in some cases AVX gives a win.
  *
  * It also depends on how many times the loop will be unrolled.
  * We are unrolling the loop 8 times (by the number of available registers), but it not always the best.
  *
  * It also depends on the usage of aligned or unaligned loads/stores.
  * We are using unaligned loads and aligned stores.
  *
  * It also depends on the usage of prefetch instructions. It makes sense on some Intel CPUs but can slow down performance on AMD.
  * Setting up correct offset for prefetching is non-obvious.
  *
  * Non-temporary (cache bypassing) stores can be used for very large sizes (more than a half of L3 cache).
  * But the exact threshold is unclear - when doing memcpy from multiple threads the optimal threshold can be lower,
  * because L3 cache is shared (and L2 cache is partially shared).
  *
  * Very large size of memcpy typically indicates suboptimal (not cache friendly) algorithms in code or unrealistic scenarios,
  * so we don't pay attention to using non-temporary stores.
  *
  * On recent Intel CPUs, the presence of "erms" makes "rep movsb" the most benefitial,
  * even comparing to non-temporary aligned unrolled stores even with the most wide registers.
  *
  * memcpy can be written in asm, C or C++. The latter can also use inline asm.
  * The asm implementation can be better to make sure that compiler won't make the code worse,
  * to ensure the order of branches, the code layout, the usage of all required registers.
  * But if it is located in separate translation unit, inlining will not be possible
  * (inline asm can be used to overcome this limitation).
  * Sometimes C or C++ code can be further optimized by compiler.
  * For example, clang is capable replacing SSE intrinsics to AVX code if -mavx is used.
  *
  * Please note that compiler can replace plain code to memcpy and vice versa.
  * - memcpy with compile-time known small size is replaced to simple instructions without a call to memcpy;
  *   it is controlled by -fbuiltin-memcpy and can be manually ensured by calling __builtin_memcpy.
  *   This is often used to implement unaligned load/store without undefined behaviour in C++.
  * - a loop with copying bytes can be recognized and replaced by a call to memcpy;
  *   it is controlled by -ftree-loop-distribute-patterns.
  * - also note that a loop with copying bytes can be unrolled, peeled and vectorized that will give you
  *   inline code somewhat similar to a decent implementation of memcpy.
  *
  * This description is up to date as of Mar 2021.
  *
  * How to test the memcpy implementation for performance:
  * 1. Test on real production workload.
  * 2. For synthetic test, see utils/memcpy-bench, but make sure you will do the best to exhaust the wide range of scenarios.
  *
  * TODO: Add self-tuning memcpy with bayesian bandits algorithm for large sizes.
  * See https://habr.com/en/company/yandex/blog/457612/
  */

#ifdef __clang__
#define compiler_builtin_memcpy __builtin_memcpy_inline
#else
#define compiler_builtin_memcpy __builtin_memcpy
#endif

template <class T, class AlignedStore, class UnalignedLoad>
__attribute__((always_inline)) inline void memcpy_large_body(char * __restrict & dst, const char * __restrict & src, size_t & size, AlignedStore aligned_store, UnalignedLoad unaligned_load)
{
    size_t padding = (-reinterpret_cast<size_t>(dst) & (sizeof(T) - 1));
    if (padding > 0)
    {
        compiler_builtin_memcpy(dst, src, sizeof(T));
        dst += padding;
        src += padding;
        size -= padding;
    }

    T cell[8];
    while (size >= sizeof(cell))
    {
#ifdef __clang__
#pragma clang loop unroll(full)
#endif
        for (size_t i = 0; i < 8; ++i)
        {
            cell[i] = unaligned_load(reinterpret_cast<const T *>(src) + i);
        }
        src += sizeof(cell);
#ifdef __clang__
#pragma clang loop unroll(full)
#endif
        for (size_t i = 0; i < 8; ++i)
        {
            aligned_store(reinterpret_cast<T *>(dst, alignof(T)) + i, cell[i]);
        }
        dst += sizeof(cell);
        size -= sizeof(cell);
    }
}

__attribute__((target("avx512f"), noinline)) static inline void memcpy_avx512_large(char * __restrict & dst, const char * __restrict & src, size_t & size)
{
    memcpy_large_body<__m512i>(dst, src, size, _mm512_store_si512, _mm512_loadu_si512);
}

__attribute__((target("avx2"), noinline)) static inline void memcpy_avx2_large(char * __restrict & dst, const char * __restrict & src, size_t & size)
{
    memcpy_large_body<__m256i>(dst, src, size, _mm256_store_si256, _mm256_loadu_si256);
}

inline void * inline_memcpy(void * __restrict dst_, const void * __restrict src_, size_t size)
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
            compiler_builtin_memcpy(dst + size - 8, src + size - 8, 8);
            compiler_builtin_memcpy(dst, src, 8);
        }
        else if (size >= 4)
        {
            /// Chunks of 4..7 bytes.
            compiler_builtin_memcpy(dst + size - 4, src + size - 4, 4);
            compiler_builtin_memcpy(dst, src, 4);
        }
        else if (size >= 2)
        {
            /// Chunks of 2..3 bytes.
            compiler_builtin_memcpy(dst + size - 2, src + size - 2, 2);
            compiler_builtin_memcpy(dst, src, 2);
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
            compiler_builtin_memcpy(dst + size - 16, src + size - 16, 16);

            /// Then we will copy every 16 bytes from the beginning in a loop.
            /// The last loop iteration will possibly overwrite some part of already copied last 16 bytes.
            /// This is Ok, similar to the code for small sizes above.
            while (size > 16)
            {
                compiler_builtin_memcpy(dst, src, 16);
                dst += 16;
                src += 16;
                size -= 16;
            }
        }
        else
        {
            // preprocess super large blocks
            if (__builtin_cpu_supports("avx512f") && size >= 8 * 64)
            {
                memcpy_avx512_large(dst, src, size);
            }
            else if (__builtin_cpu_supports("avx2") && size >= 8 * 32)
            {
                memcpy_avx2_large(dst, src, size);
            }

            // either coming back from avx512 or avx2 routine, we still need to run this to make sure that
            // we copy all the bytes correctly before last 128-byte boundary.
            memcpy_large_body<__m128i>(dst, src, size, _mm_store_si128, _mm_loadu_si128);

            // The latest remaining 0..127 bytes will be processed as usual.
            goto tail;
        }
    }

    return ret;
}

#undef compiler_builtin_memcpy
#include <common/defines.h>
#include <cpuid.h>
#include <immintrin.h>

#include <cstddef>
#include <cstdint>
#include <iterator>

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
#define tiflash_compiler_builtin_memcpy __builtin_memcpy_inline
#define TIFLASH_MEMCPY_UNROLL_FULLY _Pragma("clang loop unroll(full)")
#else
#define tiflash_compiler_builtin_memcpy __builtin_memcpy
#define TIFLASH_MEMCPY_UNROLL_FULLY _Pragma("GCC unroll 65534")
#endif

namespace memory_copy {

    enum class MediumSizeStrategy {
        MediumSizeSSE,
        MediumSizeRepMovsb
    };

    enum class HugeSizeStrategy {
        HugeSizeSSE,
        HugeSizeSSENT,
        HugeSizeRepMovsb,
        HugeSizeSSSE3Mux,
        HugeSizeVEX32,
        HugeSizeEVEX32,
        HugeSizeEVEX64
    };

    struct MemcpyConfig {
        size_t medium_size_threshold;
        size_t huge_size_threshold;
        size_t page_size;
        MediumSizeStrategy medium_size_strategy;
        HugeSizeStrategy huge_size_strategy;
    };

    extern MemcpyConfig memcpy_config;

    namespace detail {
        ALWAYS_INLINE static inline bool rep_movsb(
                void *__restrict dst,
                const void *__restrict src,
                size_t size) {
            asm volatile("rep movsb"
            : "+D"(dst), "+S"(src), "+c"(size)
            :
            : "memory");
            return dst;
        }

        ALWAYS_INLINE static inline void memcpy_sse_loop(
                char *__restrict &__restrict dst,
                char const *__restrict &__restrict src,
                size_t &size) {
            static constexpr const size_t vector_size = sizeof(__m128i);
            size_t padding = (-reinterpret_cast<uintptr_t>(dst)) & (vector_size - 1);

            /// If not aligned - we will copy first 16 bytes with unaligned stores.
            tiflash_compiler_builtin_memcpy(dst, src, vector_size);
            dst += padding;
            src += padding;
            size -= padding;


            /// Aligned unrolled copy. We will use half of available SSE registers.
            /// It's not possible to have both src and dst aligned.
            /// So, we will use aligned stores and unaligned loads.

            /// Aligned unrolled copy. We will use half of available SSE registers.
            /// It's not possible to have both src and dst aligned.
            /// So, we will use aligned stores and unaligned loads.
            __m128i c0, c1, c2, c3, c4, c5, c6, c7;

            while (size >= 128) {
                const auto *source = reinterpret_cast<const __m128i *>(src);
                auto *target = reinterpret_cast<__m128i *>(dst);
                c0 = _mm_loadu_si128(source + 0);
                c1 = _mm_loadu_si128(source + 1);
                c2 = _mm_loadu_si128(source + 2);
                c3 = _mm_loadu_si128(source + 3);
                c4 = _mm_loadu_si128(source + 4);
                c5 = _mm_loadu_si128(source + 5);
                c6 = _mm_loadu_si128(source + 6);
                c7 = _mm_loadu_si128(source + 7);
                src += 128;
                _mm_store_si128(target + 0, c0);
                _mm_store_si128(target + 1, c1);
                _mm_store_si128(target + 2, c2);
                _mm_store_si128(target + 3, c3);
                _mm_store_si128(target + 4, c4);
                _mm_store_si128(target + 5, c5);
                _mm_store_si128(target + 6, c6);
                _mm_store_si128(target + 7, c7);
                dst += 128;

                size -= 128;
            }
        }

        ALWAYS_INLINE static inline void memcpy_ssent_loop(
                char *__restrict &__restrict dst,
                char const *__restrict &__restrict src,
                size_t &size) {
            static constexpr const size_t vector_size = sizeof(__m128i);
            size_t padding = (-reinterpret_cast<uintptr_t>(dst)) & (vector_size - 1);

            /// If not aligned - we will copy first 16 bytes with unaligned stores.
            tiflash_compiler_builtin_memcpy(dst, src, vector_size);
            dst += padding;
            src += padding;
            size -= padding;


            /// Aligned unrolled copy. We will use half of available SSE registers.
            /// It's not possible to have both src and dst aligned.
            /// So, we will use aligned stores and unaligned loads.

            /// Aligned unrolled copy. We will use half of available SSE registers.
            /// It's not possible to have both src and dst aligned.
            /// So, we will use aligned stores and unaligned loads.
            __m128i c0, c1, c2, c3, c4, c5, c6, c7;

            while (size >= 128) {
                const auto *source = reinterpret_cast<const __m128i *>(src);
                auto *target = reinterpret_cast<__m128i *>(dst);
                __builtin_prefetch(source + 8);
                c0 = _mm_loadu_si128(source + 0);
                c1 = _mm_loadu_si128(source + 1);
                c2 = _mm_loadu_si128(source + 2);
                c3 = _mm_loadu_si128(source + 3);
                c4 = _mm_loadu_si128(source + 4);
                c5 = _mm_loadu_si128(source + 5);
                c6 = _mm_loadu_si128(source + 6);
                c7 = _mm_loadu_si128(source + 7);
                src += 128;
                _mm_stream_si128(target + 0, c0);
                _mm_stream_si128(target + 1, c1);
                _mm_stream_si128(target + 2, c2);
                _mm_stream_si128(target + 3, c3);
                _mm_stream_si128(target + 4, c4);
                _mm_stream_si128(target + 5, c5);
                _mm_stream_si128(target + 6, c6);
                _mm_stream_si128(target + 7, c7);
                dst += 128;

                size -= 128;
            }
        }

        template<uint8_t delta>
        __attribute__((always_inline, target("ssse3"))) static inline void memcpy_ssse3_loop(
                char *__restrict &__restrict dst,
                char const *__restrict &__restrict src,
                size_t &size) {
            src -= delta;
            size += delta;

            __m128i cell[9];
            cell[8] = _mm_load_si128(reinterpret_cast<const __m128i *>(src));
            while (size >= 144) {
                const auto *source = reinterpret_cast<const __m128i *>(__builtin_assume_aligned(src, sizeof(__m128i)));
                auto *target = reinterpret_cast<__m128i *>(__builtin_assume_aligned(dst, sizeof(__m128i)));

                __builtin_prefetch(src + 9 * 16);

                cell[0] = cell[8];
                TIFLASH_MEMCPY_UNROLL_FULLY
                for (size_t i = 1; i < std::size(cell); ++i) {
                    cell[i] = _mm_load_si128(source + i);
                }

                src += 128;

                if constexpr (delta != 0) {
                    TIFLASH_MEMCPY_UNROLL_FULLY
                    for (size_t i = 0; i < std::size(cell) - 1; ++i) {
                        cell[i] = _mm_alignr_epi8(cell[i + 1], cell[i], delta);
                    }
                }

                        TIFLASH_MEMCPY_UNROLL_FULLY
                for (size_t i = 0; i < std::size(cell) - 1; ++i) {
                    _mm_stream_si128(target + i, cell[i]);
                }
                dst += 128;
                size -= 128;
            }

            if (size >= 128) // 128 <= size <= 144, load 8 registers and store 7 registers
            {
                auto limit = 128 / sizeof(__m128i); // 8 registers
                const auto *source = reinterpret_cast<const __m128i *>(__builtin_assume_aligned(src, sizeof(__m128i)));
                auto *target = reinterpret_cast<__m128i *>(__builtin_assume_aligned(dst, sizeof(__m128i)));

                TIFLASH_MEMCPY_UNROLL_FULLY
                for (size_t i = 0; i < limit; ++i) {
                    cell[i] = _mm_load_si128(source + i);
                }

                if constexpr (delta != 0) {
                    TIFLASH_MEMCPY_UNROLL_FULLY
                    for (size_t i = 0; i < limit - 1; ++i) {
                        cell[i] = _mm_alignr_epi8(cell[i + 1], cell[i], delta);
                    }
                }

                        TIFLASH_MEMCPY_UNROLL_FULLY
                for (size_t i = 0; i < limit - 1; ++i) {
                    _mm_stream_si128(target + i, cell[i]);
                }

                src += (limit - 1) * sizeof(__m128i);
                dst += (limit - 1) * sizeof(__m128i);
                size -= (limit - 1) * sizeof(__m128i);
            }

            dst -= delta;
        }

        __attribute__((target("ssse3"))) static inline void memcpy_ssse3_mux(
                char *__restrict &__restrict dst,
                char const *__restrict &__restrict src,
                size_t &size) {
            auto dst_padding = (-reinterpret_cast<uintptr_t>(dst)) & (sizeof(__m128i) - 1);
            tiflash_compiler_builtin_memcpy(dst, src, 16);
            dst += dst_padding;
            src += dst_padding;
            size -= dst_padding;

            auto delta = reinterpret_cast<uintptr_t>(src) & 15;

            switch (delta) {
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

        template<typename Vector, size_t page_num, size_t vec_num, typename Load, typename Store>
        __attribute__((always_inline, target("avx512f,avx512vl"))) static inline void memcpy_evex_impl(
                char *__restrict &__restrict dst,
                char const *__restrict &__restrict src,
                size_t &__restrict size,
                Load load,
                Store store) {
            constexpr size_t stride_size = vec_num * sizeof(Vector);
            const auto page_size = memcpy_config.page_size;
            Vector storage[vec_num * page_num];

            while (size >= page_num * page_size) {
                for (size_t i = 0; i < page_size / stride_size; ++i) {
                    auto *target = static_cast<char *>(__builtin_assume_aligned(dst, alignof(Vector)));
                    const auto *source = static_cast<const char *>(src);

                    // prefetch one stride per-page
                    TIFLASH_MEMCPY_UNROLL_FULLY
                    for (size_t p = 0; p < page_num; ++p) {
                        __builtin_prefetch(source + page_size * p + stride_size);
                    };

                    TIFLASH_MEMCPY_UNROLL_FULLY
                    for (size_t p = 0; p < page_num; ++p) {
                        TIFLASH_MEMCPY_UNROLL_FULLY
                        for (size_t v = 0; v < vec_num; ++v) {
                            const auto *address = reinterpret_cast<const Vector *>(source + page_size * p +
                                                                                   sizeof(Vector) * v);
                            storage[p * vec_num + v] = load(address);
                        };
                    };

                    TIFLASH_MEMCPY_UNROLL_FULLY
                    for (size_t p = 0; p < page_num; ++p) {
                        TIFLASH_MEMCPY_UNROLL_FULLY
                        for (size_t v = 0; v < vec_num; ++v) {
                            auto *address = reinterpret_cast<Vector *>(target + page_size * p + sizeof(Vector) * v);
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

        template<typename Vector, size_t page_num, size_t vec_num, typename Load, typename Store>
        __attribute__((always_inline, target("avx2"))) static inline void memcpy_vex_impl(
                char *__restrict &__restrict dst,
                char const *__restrict &__restrict src,
                size_t &__restrict size,
                Load load,
                Store store) {
            constexpr size_t stride_size = vec_num * sizeof(Vector);
            const auto page_size = memcpy_config.page_size;
            Vector storage[vec_num * page_num]{};

            while (size >= page_num * page_size) {
                for (size_t i = 0; i < page_size / stride_size; ++i) {
                    auto *target = static_cast<char *>(__builtin_assume_aligned(dst, alignof(Vector)));
                    const auto *source = static_cast<const char *>(src);

                    // prefetch one stride per-page
                    TIFLASH_MEMCPY_UNROLL_FULLY
                    for (size_t p = 0; p < page_num; ++p) {
                        __builtin_prefetch(source + page_size * p + stride_size);
                    };

                    TIFLASH_MEMCPY_UNROLL_FULLY
                    for (size_t p = 0; p < page_num; ++p) {
                        TIFLASH_MEMCPY_UNROLL_FULLY
                        for (size_t v = 0; v < vec_num; ++v) {
                            const auto *address = reinterpret_cast<const Vector *>(source + page_size * p +
                                                                                   sizeof(Vector) * v);
                            storage[p * vec_num + v] = load(address);
                        };
                    };

                    TIFLASH_MEMCPY_UNROLL_FULLY
                    for (size_t p = 0; p < page_num; ++p) {
                        TIFLASH_MEMCPY_UNROLL_FULLY
                        for (size_t v = 0; v < vec_num; ++v) {
                            auto *address = reinterpret_cast<Vector *>(target + page_size * p + sizeof(Vector) * v);
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
                char *__restrict &__restrict dst,
                char const *__restrict &__restrict src,
                size_t &size) {
            auto dst_padding = (-reinterpret_cast<uintptr_t>(dst)) & (sizeof(__m256i) - 1);
            auto diff = (reinterpret_cast<uintptr_t>(dst) ^ reinterpret_cast<uintptr_t>(src)) & (sizeof(__m256i) - 1);
            tiflash_compiler_builtin_memcpy(dst, src, sizeof(__m256i));
            dst += dst_padding;
            src += dst_padding;
            size -= dst_padding;
            if (size >= 16 * memcpy_config.huge_size_threshold) {
                if (diff == 0) {
                    memcpy_vex_impl<__m256i, 4, 4>(dst, src, size, _mm256_load_si256, _mm256_stream_si256);
                } else {
                    memcpy_vex_impl<__m256i, 4, 4>(dst, src, size, _mm256_loadu_si256, _mm256_stream_si256);
                }
            } else {
                if (diff == 0) {
                    memcpy_vex_impl<__m256i, 2, 4>(dst, src, size, _mm256_load_si256, _mm256_stream_si256);
                } else {
                    memcpy_vex_impl<__m256i, 2, 4>(dst, src, size, _mm256_loadu_si256, _mm256_stream_si256);
                }
            }
            memcpy_sse_loop(dst, src, size);
        }

        __attribute__((target("avx512f,avx512vl"))) static inline void memcpy_evex32(
                char *__restrict &__restrict dst,
                char const *__restrict &__restrict src,
                size_t &size) {
            auto dst_padding = (-reinterpret_cast<uintptr_t>(dst)) & (sizeof(__m256i) - 1);
            auto diff = (reinterpret_cast<uintptr_t>(dst) ^ reinterpret_cast<uintptr_t>(src)) & (sizeof(__m256i) - 1);
            tiflash_compiler_builtin_memcpy(dst, src, sizeof(__m256i));
            dst += dst_padding;
            src += dst_padding;
            size -= dst_padding;
            if (size >= 16 * memcpy_config.huge_size_threshold) {
                if (diff == 0) {
                    memcpy_evex_impl<__m256i, 4, 4>(dst, src, size, _mm256_load_si256, _mm256_stream_si256);
                } else {
                    memcpy_evex_impl<__m256i, 4, 4>(dst, src, size, _mm256_loadu_si256, _mm256_stream_si256);
                }
            } else {
                if (diff == 0) {
                    memcpy_evex_impl<__m256i, 2, 4>(dst, src, size, _mm256_load_si256, _mm256_stream_si256);
                } else {
                    memcpy_evex_impl<__m256i, 2, 4>(dst, src, size, _mm256_loadu_si256, _mm256_stream_si256);
                }
            }
            memcpy_sse_loop(dst, src, size);
        }

        __attribute__((target("avx512f,avx512vl"))) static inline void memcpy_evex64(
                char *__restrict &__restrict dst,
                char const *__restrict &__restrict src,
                size_t &size) {
            auto dst_padding = (-reinterpret_cast<uintptr_t>(dst)) & (sizeof(__m512i) - 1);
            auto diff = (reinterpret_cast<uintptr_t>(dst) ^ reinterpret_cast<uintptr_t>(src)) & (sizeof(__m512i) - 1);
            tiflash_compiler_builtin_memcpy(dst, src, sizeof(__m512i));
            dst += dst_padding;
            src += dst_padding;
            size -= dst_padding;
            if (size >= 16 * memcpy_config.huge_size_threshold) {
                if (diff == 0) {
                    memcpy_evex_impl<__m512i, 4, 4>(dst, src, size, _mm512_load_si512, _mm512_stream_si512);
                } else {
                    memcpy_evex_impl<__m512i, 4, 4>(dst, src, size, _mm512_loadu_si512, _mm512_stream_si512);
                }
            } else {
                if (diff == 0) {
                    memcpy_evex_impl<__m512i, 2, 4>(dst, src, size, _mm512_load_si512, _mm512_stream_si512);
                } else {
                    memcpy_evex_impl<__m512i, 2, 4>(dst, src, size, _mm512_loadu_si512, _mm512_stream_si512);
                }
            }
            memcpy_sse_loop(dst, src, size);
        }

        ALWAYS_INLINE static inline bool memcpy_large(
                char *__restrict &__restrict dst,
                char const *__restrict &__restrict src,
                size_t &size) {
            if (size < memcpy_config.huge_size_threshold) {
                // medium sizes
                switch (memcpy_config.medium_size_strategy) {
                    case MediumSizeStrategy::MediumSizeRepMovsb:
                        detail::rep_movsb(dst, src, size);
                        return true;
                    default:
                        detail::memcpy_sse_loop(dst, src, size);
                }
            } else {
                // huge sizes
                switch (memcpy_config.huge_size_strategy) {
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
                        detail::memcpy_ssent_loop(dst, src, size);
                        break;
                    default:
                        detail::memcpy_sse_loop(dst, src, size);
                }
            }
            return false;
        }

    } // namespace detail

    ALWAYS_INLINE static inline bool check_valid_strategy(HugeSizeStrategy strategy) {
        int out[4];
        switch (strategy) {
            case HugeSizeStrategy::HugeSizeRepMovsb:
                // get ERMS bit from cpuid (exa=7, ebx=0)
                __cpuid_count(0x00000007, 0, out[0], out[1], out[2], out[3]);
                return (out[1] & (1 << 9)) != 0;
            case HugeSizeStrategy::HugeSizeSSSE3Mux:
                return __builtin_cpu_supports("ssse3");
            case HugeSizeStrategy::HugeSizeEVEX32:
            case HugeSizeStrategy::HugeSizeEVEX64:
                __cpuid_count(0x00000007, 0, out[0], out[1], out[2], out[3]);
                return (out[1] & (1 << 31)) != 0;
            case HugeSizeStrategy::HugeSizeVEX32:
                return __builtin_cpu_supports("avx2");
            default:
                return true;
        }
    }

    ALWAYS_INLINE static inline bool check_valid_strategy(MediumSizeStrategy strategy) {
        int out[4];
        switch (strategy) {
            case MediumSizeStrategy::MediumSizeRepMovsb:
                // get ERMS bit from cpuid (exa=7, ebx=0)
                __cpuid_count(0x00000007, 0, out[0], out[1], out[2], out[3]);
                return (out[1] & (1 << 9)) != 0;
            default:
                return true;
        }
    }

} // namespace memory_copy


ALWAYS_INLINE static inline void *
inline_memcpy(void *__restrict dst_, const void *__restrict src_, size_t size) noexcept {
    using namespace memory_copy;
    /// We will use pointer arithmetic, so char pointer will be used.
    /// Note that __restrict makes sense (otherwise compiler will reload data from memory
    /// instead of using the value of registers due to possible aliasing).
    char *__restrict dst = reinterpret_cast<char *__restrict>(dst_);
    const char *__restrict src = reinterpret_cast<const char *__restrict>(src_);

    /// Standard memcpy returns the original value of dst. It is rarely used but we have to do it.
    /// If you use memcpy with small but non-constant sizes, you can call inline_memcpy directly
    /// for inlining and removing this single instruction.
    void *ret = dst;
    tail:
    /// Small sizes and tails after the loop for large sizes.
    /// The order of branches is important but in fact the optimal order depends on the distribution of sizes in your application.
    /// This order of branches is from the disassembly of glibc's code.
    /// We copy chunks of possibly uneven size with two overlapping movs.
    /// Example: to copy 5 bytes [0, 1, 2, 3, 4] we will copy tail [1, 2, 3, 4] first and then head [0, 1, 2, 3].
    if (likely(size <= 16)) {
        if (size >= 8) {
            /// Chunks of 8..16 bytes.
            tiflash_compiler_builtin_memcpy(dst + size - 8, src + size - 8, 8);
            tiflash_compiler_builtin_memcpy(dst, src, 8);
        } else if (size >= 4) {
            /// Chunks of 4..7 bytes.
            tiflash_compiler_builtin_memcpy(dst + size - 4, src + size - 4, 4);
            tiflash_compiler_builtin_memcpy(dst, src, 4);
        } else if (size >= 2) {
            /// Chunks of 2..3 bytes.
            tiflash_compiler_builtin_memcpy(dst + size - 2, src + size - 2, 2);
            tiflash_compiler_builtin_memcpy(dst, src, 2);
        } else if (size >= 1) {
            /// A single byte.
            *dst = *src;
        }
        /// No bytes remaining.
    } else {
        if (size <= 128) {
            /// Medium size, not enough for full loop unrolling.

            /// We will copy the last 16 bytes.
            tiflash_compiler_builtin_memcpy(dst + size - 16, src + size - 16, 16);

            /// Then we will copy every 16 bytes from the beginning in a loop.
            /// The last loop iteration will possibly overwrite some part of already copied last 16 bytes.
            /// This is Ok, similar to the code for small sizes above.
            while (size > 16) {
                tiflash_compiler_builtin_memcpy(dst, src, 16);
                dst += 16;
                src += 16;
                size -= 16;
            }
        } else {
            if (size < memcpy_config.medium_size_threshold) {
                detail::memcpy_sse_loop(dst, src, size);
            } else if (detail::memcpy_large(dst, src, size)) {
                return ret;
            }
            goto tail;
        }
    }

    return ret;
}

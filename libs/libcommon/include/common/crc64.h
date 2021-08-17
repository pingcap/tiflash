#pragma once
#include <common/crc64_fast.h>
#include <common/crc64_table.h>
#include <common/simd.h>
namespace crc64
{
enum class Mode
{
    Table,
    Auto,
    SIMD_128,
    SIMD_256, // always define this, but no effect if VPCLMULQDQ support is unavailable
    SIMD_512, // always define this, but no effect if VPCLMULQDQ support is unavailable
};
class Digest
{
public:
    explicit Digest(Mode mode = Mode::Auto)
    {
        // clang-format off
        using namespace simd_option;
#if TIFLASH_COMPILER_VPCLMULQDQ_SUPPORT
        if ((mode == Mode::Auto || mode >= Mode::SIMD_512) && ENABLE_AVX512
             && __builtin_cpu_supports("vpclmulqdq") && __builtin_cpu_supports("avx512dq"))
        {
            update_fn = [](uint64_t _state, const void * _src, size_t _length) {
                return crc64::_detail::update_fast<512>(crc64::_detail::update_vpclmulqdq_avx512, _state, _src, _length);
            };
        }
        else if ((mode == Mode::Auto || mode >= Mode::SIMD_256) && ENABLE_AVX
             && __builtin_cpu_supports("vpclmulqdq") && __builtin_cpu_supports("avx2"))
        {
            update_fn = [](uint64_t _state, const void * _src, size_t _length) {
                return crc64::_detail::update_fast<256>(crc64::_detail::update_vpclmulqdq_avx2, _state, _src, _length);
            };
        }
        else
#endif
#if __SSE2__ || defined(TIFLASH_ENABLE_ASIMD_SUPPORT)
        if (mode == Mode::Auto || mode >= Mode::SIMD_128)
        {
            update_fn = [](uint64_t _state, const void * _src, size_t _length) {
                return crc64::_detail::update_fast(crc64::_detail::update_simd, _state, _src, _length);
            };
#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
            if (!ENABLE_ASIMD || !SIMDRuntimeSupport(SIMDFeature::pmull))
            {
                update_fn = _detail::update_table;
            }
#endif
#if __SSE2__
            if (!__builtin_cpu_supports("pclmul"))
            {
                update_fn = _detail::update_table;
            }
#endif
        }
        else // NOLINT(readability-misleading-indentation)
#endif
        {
            update_fn = _detail::update_table;
        }
        // clang-format on
    };

    void update(const void * src, size_t length) { state = update_fn(state, src, length); }

    [[nodiscard]] uint64_t checksum() const { return ~state; }

private:
    uint64_t (*update_fn)(uint64_t, const void *, size_t) = nullptr;

    uint64_t state = ~0ull;
};
} // namespace crc64

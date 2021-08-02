#pragma once

namespace DB
{

#ifdef __x86_64__
enum class X86SIMDLevel
{
    Disable = 0,
    SSE = 1,
    AVX = 2,
    AVX512 = 3
};

extern X86SIMDLevel TIFLASH_SIMD_LEVEL;
#endif

/// TODO: ARM Support

} // namespace DB

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
} // namespace memory_copy

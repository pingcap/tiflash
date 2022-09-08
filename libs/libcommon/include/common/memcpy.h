#pragma once

#include <common/defines.h>

#include <cstring>

#if defined(__SSE2__)
#include <common/sse2_memcpy.h>
#endif

ALWAYS_INLINE static inline void * inline_memcpy(void * __restrict dst, const void * __restrict src, size_t size)
{
#if defined(__SSE2__)
    return sse2_inline_memcpy(dst, src, size);
#else
    return std::memcpy(dst, src, size);
#endif
}

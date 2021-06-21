#include "memcpy.h"

/// This is needed to generate an object file for linking.

extern "C" void * memcpy(void * __restrict dst, const void * __restrict src, size_t size)
{
    return inline_memcpy(dst, src, size);
}

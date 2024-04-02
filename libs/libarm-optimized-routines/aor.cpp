#include "aor.h"

extern "C" __attribute__((visibility("default"))) void * memcpy(void * __restrict dst, const void * __restrict src, size_t size)
{
    return inline_memcpy(dst, src, size);
}

extern "C" __attribute__((visibility("default"))) void * memmove(void * __restrict dst, const void * src, size_t size)
{
    return inline_memmove(dst, src, size);
}

extern "C" __attribute__((visibility("default"))) void * memset(void * dst, int c, size_t size)
{
    return inline_memset(dst, c, size);
}

extern "C" __attribute__((visibility("default"))) void * memchr(void * src, int c, size_t size)
{
    return inline_memchr(src, c, size);
}

extern "C" __attribute__((visibility("default"))) void * memrchr(const void * src, int c, size_t size)
{
    return inline_memrchr(src, c, size);
}

extern "C" __attribute__((visibility("default"))) int memcmp(const void * src1, const void * src2, size_t size)
{
    return inline_memcmp(src1, src2, size);
}

extern "C" __attribute__((visibility("default"))) char * strcpy(char * __restrict dst, const char * __restrict src)
{
    return inline_strcpy(dst, src);
}

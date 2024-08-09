// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "aor.h"

extern "C" __attribute__((visibility("default"))) void * memcpy(
    void * __restrict dst,
    const void * __restrict src,
    size_t size)
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

extern "C" __attribute__((visibility("default"))) void * memchr(const void * src, int c, size_t size)
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

extern "C" __attribute__((visibility("default"))) char * strcpy(char * __restrict dst_, const char * __restrict src_)
{
    return inline_strcpy(dst_, src_);
}

extern "C" __attribute__((visibility("default"))) char * stpcpy(char * __restrict dst_, const char * __restrict src_)
{
    return inline_stpcpy(dst_, src_);
}

extern "C" __attribute__((visibility("default"))) int strcmp(const char * src1_, const char * src2_)
{
    return inline_strcmp(src1_, src2_);
}

extern "C" __attribute__((visibility("default"))) char * strchr(const char * src_, int c)
{
    return inline_strchr(src_, c);
}

extern "C" __attribute__((visibility("default"))) char * strrchr(const char * src_, int c)
{
    return inline_strrchr(src_, c);
}

extern "C" __attribute__((visibility("default"))) char * strchrnul(const char * src_, int c)
{
    return inline_strchrnul(src_, c);
}

extern "C" __attribute__((visibility("default"))) size_t strlen(const char * src_)
{
    return inline_strlen(src_);
}

extern "C" __attribute__((visibility("default"))) size_t strnlen(const char * src_, size_t size)
{
    return inline_strnlen(src_, size);
}

extern "C" __attribute__((visibility("default"))) int strncmp(const char * src1_, const char * src2_, size_t size)
{
    return inline_strncmp(src1_, src2_, size);
}

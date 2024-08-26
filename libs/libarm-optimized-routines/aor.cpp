// Copyright 2022 PingCAP, Inc.
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

__attribute__((visibility("default"))) void * memcpy(
    void * __restrict dst,
    const void * __restrict src,
    size_t size) noexcept(true)
{
    return inline_memcpy(dst, src, size);
}

__attribute__((visibility("default"))) void * memmove(void * __restrict dst, const void * src, size_t size) noexcept(true)
{
    return inline_memmove(dst, src, size);
}

__attribute__((visibility("default"))) void * memset(void * dst, int c, size_t size) noexcept(true)
{
    return inline_memset(dst, c, size);
}

__attribute__((visibility("default"))) const void * memchr(const void * src, int c, size_t size) noexcept(true)
{
    return inline_memchr(src, c, size);
}

__attribute__((visibility("default"))) void * memchr(void * src, int c, size_t size) noexcept(true)
{
    return inline_memchr(src, c, size);
}

__attribute__((visibility("default"))) const void * memrchr(const void * src, int c, size_t size) noexcept(true)
{
    return inline_memrchr(src, c, size);
}

__attribute__((visibility("default"))) void * memrchr(void * src, int c, size_t size) noexcept(true)
{
    return inline_memrchr(src, c, size);
}

__attribute__((visibility("default"))) int memcmp(const void * src1, const void * src2, size_t size) noexcept(true)
{
    return inline_memcmp(src1, src2, size);
}

__attribute__((visibility("default"))) inline char * strcpy(
    char * __restrict dst_,
    const char * __restrict src_) noexcept(true)
{
    return inline_strcpy(dst_, src_);
}

__attribute__((visibility("default"))) inline char * stpcpy(
    char * __restrict dst_,
    const char * __restrict src_) noexcept(true)
{
    return inline_stpcpy(dst_, src_);
}

__attribute__((visibility("default"))) inline int strcmp(const char * src1_, const char * src2_)  noexcept(true)
{
    return inline_strcmp(src1_, src2_);
}

__attribute__((visibility("default"))) inline const char * strchr(const char * src_, int c) noexcept(true)
{
    return inline_strchr(src_, c);
}

__attribute__((visibility("default"))) inline char * strchr(char * src_, int c) noexcept(true)
{
    return inline_strchr(src_, c);
}

__attribute__((visibility("default"))) inline const char * strrchr(const char * src_, int c) noexcept(true)
{
    return inline_strrchr(src_, c);
}

__attribute__((visibility("default"))) inline char * strrchr(char * src_, int c) noexcept(true)
{
    return inline_strrchr(src_, c);
}

__attribute__((visibility("default"))) inline const char * strchrnul(const char * src_, int c) noexcept(true)
{
    return inline_strchrnul(src_, c);
}

__attribute__((visibility("default"))) inline size_t strlen(char * src_) noexcept(true)
{
    return inline_strlen(src_);
}

__attribute__((visibility("default"))) inline size_t strnlen(const char * src_, size_t size) noexcept(true)
{
    return inline_strnlen(src_, size);
}

__attribute__((visibility("default"))) inline int strncmp(
    const char * src1_,
    const char * src2_,
    size_t size) noexcept(true)
{
    return inline_strncmp(src1_, src2_, size);
}

// Copyright 2023 PingCAP, Inc.
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

#pragma once

#include <common/defines.h>
#include <string.h>

#include <cassert>

/** memcpy function could work suboptimal if all the following conditions are met:
  * 1. Size of memory region is relatively small (approximately, under 50 bytes).
  * 2. Size of memory region is not known at compile-time.
  *
  * In that case, memcpy works suboptimal by following reasons:
  * 1. Function is not inlined.
  * 2. Much time/instructions are spend to process "tails" of data.
  *
  * There are cases when function could be implemented in more optimal way, with help of some assumptions.
  * One of that assumptions - ability to read and write some number of bytes after end of passed memory regions.
  * Under that assumption, it is possible not to implement difficult code to process tails of data and do copy always by big chunks.
  *
  * This case is typical, for example, when many small pieces of data are gathered to single contiguous piece of memory in a loop.
  * - because each next copy will overwrite excessive data after previous copy.
  *
  * Assumption that size of memory region is small enough allows us to not unroll the loop.
  * This is slower, when size of memory is actually big.
  *
  * Use with caution.
  */

namespace detail
{
ALWAYS_INLINE inline void memcpySmallAllowReadWriteOverflow15Impl(
    char * __restrict dst,
    const char * __restrict src,
    ssize_t n)
{
    while (n > 0)
    {
        tiflash_compiler_builtin_memcpy(dst, src, 16);
        dst += 16;
        src += 16;
        n -= 16;
    }
}
} // namespace detail

/** Works under assumption, that it's possible to read up to 15 excessive bytes after end of 'src' region
  *  and to write any garbage into up to 15 bytes after end of 'dst' region.
  */
__attribute__((always_inline)) inline void memcpySmallAllowReadWriteOverflow15(
    void * __restrict dst,
    const void * __restrict src,
    size_t n)
{
    ::detail::memcpySmallAllowReadWriteOverflow15Impl(static_cast<char *>(dst), static_cast<const char *>(src), n);
}

/** Works under assumptions:
  * 1. copy maximum 64 Byte.
  * 2. may read up to 15 excessive bytes after end of 'src' region.
  * 3. may write any garbage into up to 15 bytes after end of 'dst' region.
  */
__attribute__((always_inline)) inline void memcpyMax64BAllowReadWriteOverflow15(
    void * __restrict dst,
    const void * __restrict src,
    size_t n)
{
    assert(n <= 64);
    auto * d = static_cast<char *>(dst);
    const auto * s = static_cast<const char *>(src);
    switch ((n + 15) / 16)
    {
    case 4:
        tiflash_compiler_builtin_memcpy(d + 48, s + 48, 16);
        [[fallthrough]];
    case 3:
        tiflash_compiler_builtin_memcpy(d + 32, s + 32, 16);
        [[fallthrough]];
    case 2:
        tiflash_compiler_builtin_memcpy(d + 16, s + 16, 16);
        [[fallthrough]];
    case 1:
        tiflash_compiler_builtin_memcpy(d, s, 16);
    }
}

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

#include <string_view>

#ifdef TIFLASH_ENABLE_AVX_SUPPORT

#if defined(__AVX2__)

constexpr bool tiflash_use_avx2_compile_flag = true;

// if cpp source file is compiled with flag `-mavx2`, it's recommended to use inline function for better performance.
#include <common/avx2_byte_count.h>
#include <common/avx2_mem_utils.h>
#include <common/avx2_strstr.h>

#else
constexpr bool tiflash_use_avx2_compile_flag = false;
#endif

#define ASSERT_USE_AVX2_COMPILE_FLAG \
    static_assert(tiflash_use_avx2_compile_flag, __FILE__ " need compile flag `-mavx2`");

#endif

#ifdef TIFLASH_ENABLE_AVX_SUPPORT

namespace mem_utils
{

// same function like `std::string_view::find`
// - return `-1` if failed to find `needle` in `src`
// - return `0` if size of `needle` is 0
// - return the position where `needle` occur first
size_t avx2_strstr(std::string_view src, std::string_view needle);
size_t avx2_strstr(const char * src, size_t n, const char * needle, size_t k);

// same function like `std::memchr`
// - return the first address pointer which equal to target `char`
// - reeurn `nullptr` if no match
const char * avx2_memchr(const char * src, size_t n, char target);

// same function like `bcmp` or `std::memcmp(p1,p2,n) == 0`
bool avx2_mem_equal(const char * p1, const char * p2, size_t n);

// same function like `std::memcmp`
int avx2_mem_cmp(const char * p1, const char * p2, size_t n);

// return count of target byte
uint64_t avx2_byte_count(const char * src, size_t size, char target);

} // namespace mem_utils

#endif

namespace mem_utils
{

FLATTEN_INLINE_PURE static inline bool IsStrEqualWithSameSize(const char * lhs, const char * rhs, size_t size)
{
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#ifdef __AVX2__
    return mem_utils::details::avx2_mem_equal(lhs, rhs, size);
#else
    return mem_utils::avx2_mem_equal(lhs, rhs, size);
#endif
#else
    return 0 == std::memcmp(lhs, rhs, size);
#endif
}

// same function like `std::string_view == std::string_view`
FLATTEN_INLINE_PURE static inline bool IsStrViewEqual(const std::string_view & lhs, const std::string_view & rhs)
{
    if (lhs.size() != rhs.size())
        return false;

    return IsStrEqualWithSameSize(lhs.data(), rhs.data(), lhs.size());
}

// same function like `std::string_view.compare(std::string_view)`
FLATTEN_INLINE_PURE static inline int CompareStrView(const std::string_view & lhs, const std::string_view & rhs)
{
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    const size_t size = std::min(lhs.size(), rhs.size());

#ifdef __AVX2__
    int ret = mem_utils::details::avx2_mem_cmp(lhs.data(), rhs.data(), size);
#else
    int ret = mem_utils::avx2_mem_cmp(lhs.data(), rhs.data(), size);
#endif

    if (ret == 0)
    {
        ret = (lhs.size() == rhs.size()) ? 0 : (lhs.size() < rhs.size() ? -1 : 1);
    }
    return ret;
#else
    return lhs.compare(rhs);
#endif
}

// same function like `std::string_view::find(std::string_view)`
FLATTEN_INLINE_PURE static inline size_t StrFind(std::string_view src, std::string_view needle)
{
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#ifdef __AVX2__
    return mem_utils::details::avx2_strstr(src, needle);
#else
    return mem_utils::avx2_strstr(src, needle);
#endif
#else
    return src.find(needle);
#endif
}

} // namespace mem_utils
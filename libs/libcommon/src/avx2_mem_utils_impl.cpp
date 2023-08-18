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

#include <common/mem_utils_opt.h>

#ifdef TIFLASH_ENABLE_AVX_SUPPORT

ASSERT_USE_AVX2_COMPILE_FLAG

namespace mem_utils
{
size_t avx2_strstr(const char * src, size_t n, const char * needle, size_t k)
{
    return details::avx2_strstr(src, n, needle, k);
}
size_t avx2_strstr(std::string_view src, std::string_view needle)
{
    return details::avx2_strstr(src, needle);
}

bool avx2_mem_equal(const char * p1, const char * p2, size_t n)
{
    return details::avx2_mem_equal(p1, p2, n);
}

int avx2_mem_cmp(const char * p1, const char * p2, size_t n)
{
    return details::avx2_mem_cmp(p1, p2, n);
}

const char * avx2_memchr(const char * src, size_t n, char target)
{
    return details::avx2_memchr(src, n, target);
}

uint64_t avx2_byte_count(const char * src, size_t size, char target)
{
    return details::avx2_byte_count(src, size, target);
}

} // namespace mem_utils

#endif

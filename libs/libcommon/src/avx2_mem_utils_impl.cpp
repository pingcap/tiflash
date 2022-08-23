// Copyright 2022 PingCAP, Ltd.
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

#ifdef TIFLASH_ENABLE_AVX_SUPPORT

#include <common/mem_utils_opt.h>

size_t avx2_strstr(const char * src, size_t n, const char * needle, size_t k)
{
    return mem_utils::avx2_strstr(src, n, needle, k);
}
size_t avx2_strstr(std::string_view src, std::string_view needle)
{
    return mem_utils::avx2_strstr(src, needle);
}

bool avx2_mem_equal(const char * p1, const char * p2, size_t n)
{
    return mem_utils::avx2_mem_equal(p1, p2, n);
}

const char * avx2_memchr(const char * src, size_t n, char target)
{
    return mem_utils::avx2_memchr(src, n, target);
}

#endif

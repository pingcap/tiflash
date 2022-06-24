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

#include <aor.h>

#include <utility>
namespace aor
{

bool enable_sve = false;
bool enable_mte = false;

template <class SVERoutine, class MTERoutine, class ASIMDRoutine, typename... Args>
__attribute__((always_inline)) inline auto dispatch(
    SVERoutine sve_routine,
    MTERoutine mte_routine,
    ASIMDRoutine asimd_routine,
    Args &&... args)
{
    if (enable_sve)
        return sve_routine(std::forward<Args>(args)...);
    if (enable_mte)
        return mte_routine(std::forward<Args>(args)...);
    return asimd_routine(std::forward<Args>(args)...);
}
} // namespace aor

extern "C" {
#include <stringlib.h>
void * memcpy(void * __restrict dst, const void * __restrict src, size_t size)
{
    return aor::dispatch(__memcpy_aarch64_sve, __memcpy_aarch64_simd, __memcpy_aarch64_simd, dst, src, size);
}
void * memmove(void * dst, const void * src, size_t size)
{
    return aor::dispatch(__memmove_aarch64_sve, __memmove_aarch64_simd, __memmove_aarch64_simd, dst, src, size);
}
void * memset(void * target, int value, size_t size)
{
    return aor::dispatch(__memset_aarch64, __memset_aarch64, __memset_aarch64, target, value, size);
}
void * memchr(const void * haystack, int needle, size_t size)
{
    return aor::dispatch(__memchr_aarch64_sve, __memchr_aarch64_mte, __memchr_aarch64, haystack, needle, size);
}
void * memrchr(const void * haystack, int needle, size_t size)
{
    return aor::dispatch(__memrchr_aarch64, __memrchr_aarch64, __memrchr_aarch64, haystack, needle, size);
}
int memcmp(const void * a, const void * b, size_t size)
{
    return aor::dispatch(__memcmp_aarch64_sve, __memcmp_aarch64, __memcmp_aarch64, a, b, size);
}
char * strcpy(char * __restrict dst, const char * __restrict src)
{
    return aor::dispatch(__strcpy_aarch64_sve, __strcpy_aarch64, __strcpy_aarch64, dst, src);
}
char * stpcpy(char * __restrict dst, const char * __restrict src)
{
    return aor::dispatch(__stpcpy_aarch64_sve, __stpcpy_aarch64, __stpcpy_aarch64, dst, src);
}
int strcmp(const char * a, const char * b)
{
    return aor::dispatch(__strcmp_aarch64_sve, __strcmp_aarch64, __strcmp_aarch64, a, b);
}
char * strchr(const char * haystack, int needle)
{
    return aor::dispatch(__strchr_aarch64_sve, __strchr_aarch64_mte, __strchr_aarch64, haystack, needle);
}
char * strrchr(const char * haystack, int needle)
{
    return aor::dispatch(__strrchr_aarch64_sve, __strrchr_aarch64_mte, __strrchr_aarch64, haystack, needle);
}
char * strchrnul(const char * haystack, int needle)
{
    return aor::dispatch(__strchrnul_aarch64_sve, __strchrnul_aarch64_mte, __strchrnul_aarch64, haystack, needle);
}
size_t strlen(const char * data)
{
    return aor::dispatch(__strlen_aarch64_sve, __strlen_aarch64_mte, __strlen_aarch64, data);
}
size_t strnlen(const char * data, size_t size)
{
    return aor::dispatch(__strnlen_aarch64_sve, __strnlen_aarch64, __strnlen_aarch64, data, size);
}
int strncmp(const char * a, const char * b, size_t size)
{
    return aor::dispatch(__strncmp_aarch64_sve, __strncmp_aarch64, __strncmp_aarch64, a, b, size);
}
}
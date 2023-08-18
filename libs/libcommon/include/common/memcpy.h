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

#include <cstring>

#if defined(__SSE2__)
#include <common/sse2_memcpy.h>
#endif
#if defined(__AVX2__)
#include <common/avx2_memcpy.h>
#endif

ALWAYS_INLINE static inline void * inline_memcpy(void * __restrict dst, const void * __restrict src, size_t size)
{
#if defined(__AVX2__)
    return mem_utils::avx2_inline_memcpy(dst, src, size);
#elif defined(__SSE2__)
    return sse2_inline_memcpy(dst, src, size);
#else
    return std::memcpy(dst, src, size);
#endif
}

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

#ifndef NO_TIFLASH_INTERNAL_MEMCPY

#if defined(__SSE2__)

#include <common/sse2_memcpy.h>

/// This is needed to generate an object file for linking.

extern "C" __attribute__((visibility("default"))) void * memcpy(void * __restrict dst, const void * __restrict src, size_t size)
{
    return sse2_inline_memcpy(dst, src, size);
}

#endif

#endif
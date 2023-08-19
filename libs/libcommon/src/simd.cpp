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

#include <common/simd.h>

namespace simd_option
{
#ifdef __x86_64__

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
bool ENABLE_AVX = true;
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
bool ENABLE_AVX512 = true;
#endif

#elif defined(__aarch64__)

#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
bool ENABLE_ASIMD = false;
#endif

#ifdef TIFLASH_ENABLE_SVE_SUPPORT
bool ENABLE_SVE = false;
#endif


#endif
} // namespace simd_option

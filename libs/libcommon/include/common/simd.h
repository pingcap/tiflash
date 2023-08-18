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
#include <common/detect_features.h>
namespace simd_option
{
#if defined(__x86_64__)

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
extern bool ENABLE_AVX;
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
extern bool ENABLE_AVX512;
#endif

#elif defined(__aarch64__)

#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
extern bool ENABLE_ASIMD;
#endif

#ifdef TIFLASH_ENABLE_SVE_SUPPORT
extern bool ENABLE_SVE;
#endif
#endif

/// @todo: notice that currently we use plain SIMD without OOP abstraction:
///     this gives several issues:
///     - there may be similar code paragraph for each vectorization extension
///     - this forbids passing SIMD type to template argument since GCC will give
///       off warnings on discard attributes
///     - some binary operations are ugly
///     For future improvement, one should wrap SIMD types into structs/classes and
///     https://gcc.gnu.org/onlinedocs/gcc/Vector-Extensions.html also gives a good example
///     to reduce some burden of type-casting.
} // namespace simd_option

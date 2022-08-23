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

#pragma once

#include <string_view>


#ifdef TIFLASH_ENABLE_AVX_SUPPORT

// same function like `std::string_view::find`
// - return `-1` if failed to find `needle` in `src`
// - return `0` if size of `needle` is 0
// - return the position where `needle` occur first
size_t avx2_strstr(std::string_view src, std::string_view needle);
size_t avx2_strstr(const char * src, size_t n, const char * needle, size_t k);
const char * avx2_memchr(const char * src, size_t n, char target);

bool avx2_mem_equal(const char * p1, const char * p2, size_t n);

#if defined(__AVX2__)

#define TIFLASH_USE_AVX2_COMPILE_FLAG 1

// if cpp source file is compiled with flag `-mavx2`, it's recommended to use inline function for better performance.
#include <common/avx2_mem_utils.h>
#include <common/avx2_strstr.h>

#endif

#endif
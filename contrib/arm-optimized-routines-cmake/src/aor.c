// Copyright 2022 PingCAP, Inc.
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

#include <stdbool.h>
#include <string.h>
#include <stringlib.h>

#undef memcpy
#undef memmove
#undef memset
#undef memchr
#undef memrchr
#undef memcmp
#undef strcpy
#undef stpcpy
#undef strcmp
#undef strchr
#undef strrchr
#undef strchrnul
#undef strlen
#undef strnlen
#undef strncmp

#define memcpy __memcpy_aarch64_simd
#define memmove __memmove_aarch64_simd
#define memset __memset_aarch64
#define memchr __memchr_aarch64
#define memrchr __memrchr_aarch64
#define memcmp __memcmp_aarch64
#define strcpy __strcpy_aarch64
#define stpcpy __stpcpy_aarch64
#define strcmp __strcmp_aarch64
#define strchr __strchr_aarch64
#define strrchr __strrchr_aarch64
#define strchrnul __strchrnul_aarch64
#define strlen __strlen_aarch64
#define strnlen __strnlen_aarch64
#define strncmp __strncmp_aarch64

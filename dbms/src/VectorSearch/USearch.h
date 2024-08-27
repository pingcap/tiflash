// Copyright 2024 PingCAP, Inc.
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

// USearch is header only. We don't use cmake to make these defines to avoid
// polluting all compile units.

#define USEARCH_USE_SIMSIMD 1
#define SIMSIMD_NATIVE_F16 0
#define SIMSIMD_NATIVE_BF16 0

// Force enable all target features.
#define SIMSIMD_TARGET_NEON 1
#define SIMSIMD_TARGET_SVE 0 // Clang13's header does not support enableing SVE for region
#define SIMSIMD_TARGET_HASWELL 1
#define SIMSIMD_TARGET_SKYLAKE 0 // Clang13 does not support AVX512
#define SIMSIMD_TARGET_ICE 0
#define SIMSIMD_TARGET_GENOA 0
#define SIMSIMD_TARGET_SAPPHIRE 0

#if __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpass-failed"

#include <type_traits>
#include <usearch/index.hpp>
#include <usearch/index_dense.hpp>
#include <usearch/index_plugins.hpp>

#pragma clang diagnostic pop
#endif

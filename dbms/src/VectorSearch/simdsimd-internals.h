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

// SIMSIMD is header only. We don't use cmake to make these defines to avoid
// polluting all compile units.

#pragma once

// Note: Be careful that usearch also includes simsimd with a customized config.
// Don't include simsimd and usearch at the same time. Otherwise, the effective
// config depends on the include order.
#define SIMSIMD_NATIVE_F16 0
#define SIMSIMD_NATIVE_BF16 0
#define SIMSIMD_DYNAMIC_DISPATCH 0

// Force enable all target features. We will do our own dynamic dispatch.
#define SIMSIMD_TARGET_NEON 1
#define SIMSIMD_TARGET_SVE 1
#define SIMSIMD_TARGET_HASWELL 1
#define SIMSIMD_TARGET_SKYLAKE 1
#define SIMSIMD_TARGET_ICE 0
#define SIMSIMD_TARGET_GENOA 0
#define SIMSIMD_TARGET_SAPPHIRE 0
#include <simsimd/simsimd.h>


namespace simsimd_details
{

simsimd_capability_t simd_capabilities();

simsimd_capability_t actual_capability(simsimd_datatype_t data_type, simsimd_metric_kind_t kind);

} // namespace simsimd_details

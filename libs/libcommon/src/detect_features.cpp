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

#include <common/detect_features.h>

namespace common
{
#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef __APPLE__
// FIXME: use `cpu_features::GetAarch64Info()` when `cpu_features` is ready
const CPUInfo cpu_info = {};
#else
const CPUInfo cpu_info = cpu_features::GetAarch64Info();
#endif
#endif

#ifdef CPU_FEATURES_ARCH_X86
const CPUInfo cpu_info = cpu_features::GetX86Info();
#endif
} // namespace common
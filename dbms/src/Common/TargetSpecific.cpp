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

#include <Common/CpuId.h>
#include <Common/TargetSpecific.h>

namespace DB
{

UInt32 getSupportedArchs()
{
    UInt32 result = 0;
    if (Cpu::CpuFlagsCache::have_SSE42)
        result |= static_cast<UInt32>(TargetArch::SSE4);
    if (Cpu::CpuFlagsCache::have_AVX)
        result |= static_cast<UInt32>(TargetArch::AVX);
    if (Cpu::CpuFlagsCache::have_AVX2)
        result |= static_cast<UInt32>(TargetArch::AVX2);
    if (Cpu::CpuFlagsCache::have_AVX512F)
        result |= static_cast<UInt32>(TargetArch::AVX512F);
    if (Cpu::CpuFlagsCache::have_AVX512BW)
        result |= static_cast<UInt32>(TargetArch::AVX512BW);
    if (Cpu::CpuFlagsCache::have_AVX512VBMI)
        result |= static_cast<UInt32>(TargetArch::AVX512VBMI);
    if (Cpu::CpuFlagsCache::have_AVX512VBMI2)
        result |= static_cast<UInt32>(TargetArch::AVX512VBMI2);
    return result;
}

bool isArchSupported(TargetArch arch)
{
    static UInt32 arches = getSupportedArchs();
    return arch == TargetArch::Default || (arches & static_cast<UInt32>(arch));
}

String toString(TargetArch arch)
{
    switch (arch)
    {
    case TargetArch::Default:
        return "default";
    case TargetArch::SSE4:
        return "sse4";
    case TargetArch::AVX:
        return "avx";
    case TargetArch::AVX2:
        return "avx2";
    case TargetArch::AVX512F:
        return "avx512f";
    case TargetArch::AVX512BW:
        return "avx512bw";
    case TargetArch::AVX512VBMI:
        return "avx512vbmi";
    case TargetArch::AVX512VBMI2:
        return "avx512vbmi";
    }

    __builtin_unreachable();
}

} // namespace DB

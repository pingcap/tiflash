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

#if defined(__aarch64__) || defined(__arm64__) || defined(__arm64) || defined(__ARM64) || defined(__AARCH64__)
#ifndef __APPLE__
#include <cpuinfo_aarch64.h>
namespace common
{
using CPUFeatures = cpu_features::Aarch64Features;
using CPUFeature = cpu_features::Aarch64FeaturesEnum;
using CPUInfo = cpu_features::Aarch64Info;
extern const CPUInfo cpu_info;
static inline const CPUFeatures & cpu_feature_flags = cpu_info.features;
} // namespace common
#else
#include <sys/sysctl.h>
#include <sys/types.h>
namespace common
{
static inline int detect(const char * name)
{
    int enabled;
    size_t enabled_len = sizeof(enabled);
    const int failure = ::sysctlbyname(name, &enabled, &enabled_len, NULL, 0);
    return failure ? 0 : enabled;
}
static inline const struct CPUFeatures
{
    bool asimd = detect("hw.optional.neon");
    bool sve = false; // currently not supported
    bool sve2 = false; // currently not supported

    bool atomics = detect("hw.optional.armv8_1_atomics");
    bool hpfp = detect("hw.optional.neon_hpfp");
    bool fp16 = detect("hw.optional.neon_fp16");
    bool fhm = detect("hw.optional.armv8_2_fhm");
    bool crc32 = detect("hw.optional.armv8_crc32");
    bool sha512 = detect("hw.optional.armv8_2_sha512");
    bool sha3 = detect("hw.optional.armv8_2_sha3");

    // the followings are assumed to be true.
    // see https://github.com/golang/sys/pull/114/files
    bool cpuid = true;
    bool aes = true;
    bool pmull = true;
    bool sha1 = true;
    bool sha2 = true;
} cpu_feature_flags;
} // namespace common
#endif
#endif

#if defined(__x86_64__) || defined(__x86_64) || defined(__amd64) || defined(__amd64__)
#include <cpuinfo_x86.h>
namespace common
{
using CPUFeatures = cpu_features::X86Features;
using CPUFeature = cpu_features::X86FeaturesEnum;
using CPUInfo = cpu_features::X86Info;
extern const CPUInfo cpu_info;
static inline const CPUFeatures & cpu_feature_flags = cpu_info.features;
} // namespace common
#endif

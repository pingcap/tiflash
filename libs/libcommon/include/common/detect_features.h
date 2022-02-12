#pragma once

#if defined(__aarch64__) || defined(__arm64__) || defined(__arm64) || defined(__ARM64) || defined(__AARCH64__)
#ifdef __APPLE__ // TODO: remove mocked CPU feature for Darwin/aarch64 when `cpu_features` supports it
namespace common
{
struct CPUFeatures
{
    int asimd : 1 = 0;
    int pmull : 1 = 0;
};
enum CPUFeature
{
    AARCH64_ASIMD,
    AARCH64_PMULL
};
struct CPUInfo
{
    CPUFeatures features;
};
extern const CPUInfo cpu_info;
static inline const CPUFeatures & cpu_feature_flags = cpu_info.features;
} // namespace common
#else
#include <cpuinfo_aarch64.h>
namespace common
{
using CPUFeatures = cpu_features::Aarch64Features;
using CPUFeature = cpu_features::Aarch64FeaturesEnum;
using CPUInfo = cpu_features::Aarch64Info;
extern const CPUInfo cpu_info;
static inline const CPUFeatures & cpu_feature_flags = cpu_info.features;
#endif
} // namespace common
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

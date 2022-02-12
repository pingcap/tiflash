#include <common/detect_features.h>

namespace common
{
#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef __APPLE__
const CPUInfo cpu_info = {};
#else
const CPUInfo cpu_info = cpu_features::GetAarch64Info();
#endif
#endif

#ifdef CPU_FEATURES_ARCH_X86
const CPUInfo cpu_info = cpu_features::GetX86Info();
#endif
} // namespace common
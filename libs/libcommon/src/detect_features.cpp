#include <common/detect_features.h>
namespace common
{
using namespace cpu_features;

#ifdef CPU_FEATURES_ARCH_X86
static const X86Info info = GetX86Info();

bool cpu_supports(CPUFeature feature) noexcept
{
    return GetX86FeaturesEnumValue(&info.features, feature) != 0;
}
#endif

#ifdef CPU_FEATURES_ARCH_AARCH64
static const cpu_features::Aarch64Info info = GetAarch64Info();

bool cpu_supports(CPUFeature feature) noexcept
{
    return GetAarch64FeaturesEnumValue(&info.features, feature) != 0;
}
#endif
} // namespace common
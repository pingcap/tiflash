#include <common/aba.h>
#include <common/detect_features.h>

#include <boost/fiber/detail/cpu_relax.hpp>
void common::aba_generic_cpu_relax()
{
    cpu_relax();
}

bool common::aba_runtime_has_xchg16b()
{
#if defined(__aarch64__)
    return cpu_feature_flags.atomics;
#elif defined(__x86_64__)
    return cpu_feature_flags.cx16;
#else
    return false;
#endif
}

bool common::aba_xchg16b(std::atomic<common::Linked> & src,
                         common::Linked & cmp,
                         common::Linked with)
{
    return src.compare_exchange_weak(
        cmp,
        with,
        std::memory_order_acq_rel,
        std::memory_order_relaxed);
}

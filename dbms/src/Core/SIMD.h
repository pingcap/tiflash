#pragma once
#if defined(__aarch64__)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif
namespace DB
{
namespace SIMDOption
{
#if defined(__x86_64__)

#ifdef DBMS_ENABLE_AVX_SUPPORT
extern bool ENABLE_AVX;
#endif

#ifdef DBMS_ENABLE_AVX512_SUPPORT
extern bool ENABLE_AVX512;
#endif

/// @attention: we keeps the SIMDFeature encoding consistent,
/// so even if the feature is not enabled, we still define a enum item for it
enum class SIMDFeature
{
    avx,
    avx2,
    avx512f,
    avx512dq,
    avx512cd,
    avx512bw,
    avx512vl
};

static inline bool SIMDRuntimeSupport(SIMDFeature feature)
{
    // https://gcc.gnu.org/onlinedocs/gcc/x86-Built-in-Functions.html
#define CHECK_RETURN(X)  \
    case SIMDFeature::X: \
        return __builtin_cpu_supports(#X);

    switch (feature)
    {
        CHECK_RETURN(avx)
        CHECK_RETURN(avx2)
        CHECK_RETURN(avx512f)
        CHECK_RETURN(avx512dq)
        CHECK_RETURN(avx512cd)
        CHECK_RETURN(avx512bw)
        CHECK_RETURN(avx512vl)
    }
#undef CHECK_RETURN
    return false;
}

#elif defined(__aarch64__)

#ifdef DBMS_ENABLE_ASIMD_SUPPORT
extern bool ENABLE_ASIMD;
#endif

#ifdef DBMS_ENABLE_SVE_SUPPORT
extern bool ENABLE_SVE;
#endif

enum class SIMDFeature
{
    asimd,
    sve,
    sve2
};


static inline bool SIMDRuntimeSupport(SIMDFeature feature)
{
    unsigned long hwcap;
    switch (feature)
    {
        // https://github.com/TrenchBoot/linux/blob/master/arch/arm64/include/uapi/asm/hwcap.h
        case SIMDFeature::sve:
        case SIMDFeature::asimd:
            hwcap = getauxval(AT_HWCAP);
            return hwcap & (feature == SIMDFeature::sve ? HWCAP_SVE : HWCAP_ASIMD);
        case SIMDFeature::sve2:
            hwcap = getauxval(AT_HWCAP2);
            return hwcap & HWCAP2_SVE2;
    }
    return false;
}
#endif

}; // namespace SIMDOption
} // namespace DB
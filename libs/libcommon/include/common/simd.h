#pragma once
#if defined(__aarch64__)
#if __has_include(<asm/hwcap.h>)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif
#endif
namespace simd_option
{
#if defined(__x86_64__)

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
extern bool ENABLE_AVX;
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
extern bool ENABLE_AVX512;
#endif

/// @attention: we keeps the SIMDFeature encoding consistent,
/// so even if the feature is not enabled, we still define an enum item for it
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
    // Please Check:
    // https://gcc.gnu.org/onlinedocs/gcc/x86-Built-in-Functions.html
    // GCC itself provides runtime feature checking builtins, but notice
    // that GCC 7 does not yet support `avx512vnni`. Since it is not very useful
    // up to current stage, we can ignore it for now.
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

#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
extern bool ENABLE_ASIMD;
#endif

#ifdef TIFLASH_ENABLE_SVE_SUPPORT
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
    /// Notice that we do not detect support for Darwin/arm64 since
    /// it does not have HWCAP support. However, if such feature is
    /// ever needed in the future, a good reference can be:
    /// https://github.com/golang/sys/pull/114
#if __has_include(<asm/hwcap.h>)
    unsigned long hwcap;
    switch (feature)
    {
        // Please Check:
        // https://github.com/torvalds/linux/blob/master/arch/arm64/include/uapi/asm/hwcap.h
        // AARCH64 targets does not has similar builtins for runtime checking; however, it has
        // a full series of HWCAP flags to achieve the similar capability.
        // CentOS 7 default kernel does not support SVE2, so we just ignore SVE2 in that case.
        // (Maybe one can consider it after ARMv9 becomes more prevalent.)
        case SIMDFeature::sve:
        case SIMDFeature::asimd:
            hwcap = getauxval(AT_HWCAP);
            return hwcap & (feature == SIMDFeature::sve ? HWCAP_SVE : HWCAP_ASIMD);
        case SIMDFeature::sve2:
#ifdef HWCAP2_SVE2
            hwcap = getauxval(AT_HWCAP2);
            return hwcap & HWCAP2_SVE2;
#else
            return false;
#endif // HWCAP2_SVE2
    }
#else
    (void)feature;
#endif // __has_include(<asm/hwcap.h>)
    return false;
}
#endif

}; // namespace simd_option

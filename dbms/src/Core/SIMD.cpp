#include "SIMD.h"
namespace DB
{
namespace SIMDOption
{
#ifdef __x86_64__

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
bool ENABLE_AVX = true;
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
bool ENABLE_AVX512 = true;
#endif

#elif defined(__aarch64__)

#ifdef DBMS_ENABLE_ASIMD_SUPPORT
bool ENABLE_ASIMD = false;
#endif

#ifdef DBMS_ENABLE_SVE_SUPPORT
bool ENABLE_SVE = false;
#endif


#endif
}; // namespace SIMDOption
} // namespace DB
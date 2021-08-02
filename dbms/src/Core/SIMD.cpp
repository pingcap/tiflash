#include <Core/SIMD.h>

namespace DB {

X86SIMDLevel TIFLASH_SIMD_LEVEL = X86SIMDLevel::AVX512; // detect from the highest level

}

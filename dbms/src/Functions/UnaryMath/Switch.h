#pragma once

#if defined(__linux__) && (defined(__aarch64__) || defined(__x86_64__))
#define TIFLASH_HAS_UNARY_MATH_VECTORIZATION_SUPPORT
namespace DB::UnaryMath
{
void disableVectorization();
void enableVectorization();
} // namespace DB::UnaryMath
#endif
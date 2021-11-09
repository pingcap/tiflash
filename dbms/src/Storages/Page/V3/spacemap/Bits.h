#pragma once

#include <Core/Types.h>

#ifdef __cplusplus
extern "C" {
#endif

int set_bit(unsigned int nr,void * addr);
int clear_bit(unsigned int nr, void * addr);
int test_bit(unsigned int nr, const void * addr);
int set_bit64(UInt64 nr, void * addr);
int clear_bit64(UInt64 nr, void * addr);
int test_bit64(UInt64 nr, const void * addr);
unsigned int div_ceil(UInt64 a, UInt64 b);

#ifdef __cplusplus
} // extern "C"
#endif

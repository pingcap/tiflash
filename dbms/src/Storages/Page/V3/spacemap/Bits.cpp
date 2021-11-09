#include "Bits.h"

#ifdef __cplusplus
extern "C" {
#endif


int set_bit(unsigned int nr, void *addr)
{
	int mask, retval;
	unsigned char *ADDR = (unsigned char *)addr;

	ADDR += nr >> 3;
	mask = 1 << (nr & 0x07);
	retval = mask & *ADDR;
	*ADDR |= mask;
	return retval;
}

int clear_bit(unsigned int nr, void *addr)
{
	int mask, retval;
	unsigned char *ADDR = (unsigned char *)addr;

	ADDR += nr >> 3;
	mask = 1 << (nr & 0x07);
	retval = mask & *ADDR;
	*ADDR &= ~mask;
	return retval;
}

int test_bit(unsigned int nr, const void *addr)
{
	int mask;
	const unsigned char *ADDR = (const unsigned char *)addr;

	ADDR += nr >> 3;
	mask = 1 << (nr & 0x07);
	return (mask & *ADDR);
}

/*
 * C-only 64 bit ops.
 */
int set_bit64(UInt64 nr, void *addr)
{
	int mask, retval;
	unsigned char *ADDR = (unsigned char *)addr;

	ADDR += nr >> 3;
	mask = 1 << (nr & 0x07);
	retval = mask & *ADDR;
	*ADDR |= mask;
	return retval;
}

int clear_bit64(UInt64 nr, void *addr)
{
	int mask, retval;
	unsigned char *ADDR = (unsigned char *)addr;

	ADDR += nr >> 3;
	mask = 1 << (nr & 0x07);
	retval = mask & *ADDR;
	*ADDR &= ~mask;
	return retval;
}

int test_bit64(UInt64 nr, const void *addr)
{
	int mask;
	const unsigned char *ADDR = (const unsigned char *)addr;

	ADDR += nr >> 3;
	mask = 1 << (nr & 0x07);
	return (mask & *ADDR);
}

unsigned int div_ceil(UInt64 a, UInt64 b)
{
	if (!a)
		return 0;
	return ((a - 1) / b) + 1;
}



#ifdef __cplusplus
}
#endif
#pragma once
#include <Core/Types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BMAP64_BITARRAY		1
#define BMAP64_RBTREE		2
#define BMAP64_AUTODIR		3

#define container_of(ptr, type, member) ({			\
		const __typeof__( ((type *)0)->member ) *__mptr = (ptr);	\
		(type *)( (char *)__mptr - offsetof(type,member) );})

struct bitmaps 
{
	struct bitmap_ops *bitmap_ops;
	int flags;
	UInt64 start, end;
	UInt64 real_end;
	int cluster_bits;
	char *description;
	void *_private;
};


int init_bitmaps(struct bitmaps *bitmap, int type, UInt64 start, UInt64 end, UInt64 real_end);
void free_bitmaps(struct bitmaps *bitmap);

int unmark_bmap(struct bitmaps  *bitmap, UInt64 arg);
int mark_bmap(struct bitmaps  *bitmap, UInt64 arg);
int unmark_block_bmap_range(struct bitmaps  *bitmap, UInt64 block, unsigned int num);
int mark_block_bmap_range(struct bitmaps  *bitmap, UInt64 block, unsigned int num);
int test_bmap(struct bitmaps  *bitmap, UInt64 arg);
int test_bmap_range(struct bitmaps  *bitmap, UInt64 block, unsigned int num);
int get_free_blocks(struct bitmaps  *bitmap,UInt64 start, UInt64 finish, int num, UInt64 *ret);
int set_bmap_range(struct bitmaps *bitmap, UInt64 start, unsigned int num, void *in);
int get_bmap_range(struct bitmaps *bitmap, UInt64 start, unsigned int num, void *out);

struct bitmap_ops {
	int	type;

	/* Generic bmap operators */
	int (*new_bmap)(struct bitmaps* bmap);
	void (*free_bmap)(struct bitmaps* bitmap);
	int (*copy_bmap)(struct bitmaps* src, struct Bitmap* dest);
	int (*resize_bmap)(struct bitmaps* bitmap, UInt64 new_end, UInt64 new_real_end);
	/* bit set/test operators */
	int	(*mark_bmap)(struct bitmaps* bitmap, UInt64 arg);
	int	(*unmark_bmap)(struct bitmaps* bitmap, UInt64 arg);
	int	(*test_bmap)(struct bitmaps* bitmap, UInt64 arg);
	void (*mark_bmap_extent)(struct bitmaps* bitmap, UInt64 arg, unsigned int num);
	void (*unmark_bmap_extent)(struct bitmaps* bitmap, UInt64 arg, unsigned int num);
	int	(*test_clear_bmap_extent)(struct bitmaps* bitmap, UInt64 arg, unsigned int num);
	int (*set_bmap_range)(struct bitmaps* bitmap, UInt64 start, size_t num, void *in);
	int (*get_bmap_range)(struct bitmaps* bitmap, UInt64 start, size_t num, void *out);
	void (*clear_bmap)(struct bitmaps* bitmap);
	void (*print_stats)(struct bitmaps* bitmap);

	/* Find the first zero bit between start and end, inclusive.
	 * May be NULL, in which case a generic function is used. */
	int (*find_first_zero)(struct bitmaps* bitmap, UInt64 start, UInt64 end, UInt64 *out);

	/* Find the first set bit between start and end, inclusive.
	 * May be NULL, in which case a generic function is used. */
	int (*find_first_set)(struct bitmaps* bitmap, UInt64 start, UInt64 end, UInt64 *out);
};

extern struct bitmap_ops bmap_rbtree;

#ifdef __cplusplus
} // extern "C"
#endif

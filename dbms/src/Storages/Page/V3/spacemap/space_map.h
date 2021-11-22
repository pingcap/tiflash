#pragma once
#include <Core/Types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SMAP64_RBTREE 1
#define SMAP64_BITARRAY 2
#define SMAP64_AUTODIR 3

#define container_of(ptr, type, member) ({          \
        const __typeof__( ((type *)0)->member ) *__mptr = (ptr);    \
        (type *)( (char *)__mptr - offsetof(type,member) ); })

struct spacemap_ops
{
    int type;
    //  void (*set_space_callback)(struct spacemap * smap);

    /* Generic space map operators */
    int (*new_smap)(struct spacemap * smap);
    /* The difference between clear and free is that after you clear, you can still use this spacemap. */
    void (*clear_smap)(struct spacemap * smap);
    void (*free_smap)(struct spacemap * smap);
    int (*copy_smap)(struct spacemap * src, struct spacemap * dest);
    int (*resize_smap)(struct spacemap * smap, UInt64 new_end, UInt64 new_real_end);

    /* print space maps status  */
    void (*print_stats)(struct spacemap * smap);

    /* space map bit/bits test operators */
    int (*test_smap_bit)(struct spacemap * smap, UInt64 block);
    int (*test_smap_range)(struct spacemap * smap, UInt64 block, unsigned int num);

    /* search range , return the free bits */
    void (*search_smap_range)(struct spacemap * smap, UInt64 start, UInt64 end, size_t num, UInt64 * ret);

    /* Find the first zero/set bit between start and end, inclusive.
     * May be NULL, in which case a generic function is used. */
    int (*find_smap_first_zero)(struct spacemap * smap, UInt64 start, UInt64 end, UInt64 * out);
    int (*find_smap_first_set)(struct spacemap * smap, UInt64 start, UInt64 end, UInt64 * out);

    /* space map bit set/unset operators */
    int (*mark_smap_bit)(struct spacemap * smap, UInt64 block);
    int (*unmark_smap_bit)(struct spacemap * smap, UInt64 block);

    /* space map range set/unset operators */
    int (*mark_smap_range)(struct spacemap * smap, UInt64 block, unsigned int num);
    int (*unmark_smap_range)(struct spacemap * smap, UInt64 block, unsigned int num);
};

struct spacemap
{
    struct spacemap_ops * spacemap_ops;
    int flags;
    /* [left - right] range */
    UInt64 start, end;
    /* limit range */
    UInt64 real_end;
    /* shift */
    int cluster_bits;
    char * description;
    void * _private;
};

extern struct spacemap_ops smap_rbtree;

void printf_smap(struct spacemap * smap);

/**
 * ret value :
 *  -1: Invalid args
 *   0: Unmark the marked node
 *   1: Unmark the unmarked node
 */
int unmark_smap(struct spacemap * smap, UInt64 arg);

/**
 * ret value :
 *  -1: Invalid args
 *   1: Mark success
 */
int mark_smap(struct spacemap * smap, UInt64 arg);

/**
 * The return value same as `unmark_smap`
 */
int unmark_smap_range(struct spacemap * smap, UInt64 block, unsigned int num);

/**
 * The return value same as `mark_smap`
 */
int mark_smap_range(struct spacemap * smap, UInt64 block, unsigned int num);

/**
 * ret value :
 *  -1: Invalid args
 *   0: This bit is marked
 *   1: This bit is not marked
 */
int test_smap(struct spacemap * smap, UInt64 arg);

/**
 * The return value same as `mark_smap`
 */
int test_smap_range(struct spacemap * smap, UInt64 block, unsigned int num);

/**
 * ret value :
 *  -1: Invalid args
 *   0: init success
 */
int init_space_map(struct spacemap * smap, int type, UInt64 start, UInt64 end, UInt64 real_end);
void destory_space_map(struct spacemap * smap);

#ifdef __cplusplus
} // extern "C"
#endif

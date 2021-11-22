#include <Core/Types.h>
#include <common/likely.h>
#ifdef __cplusplus
extern "C" {
#endif

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "space_map.h"

void printf_smap(struct spacemap * smap)
{
    if (unlikely(!smap))
    {
        return;
    }
    smap->spacemap_ops->print_stats(smap);
}

int unmark_smap(struct spacemap * smap, UInt64 arg)
{
    if (unlikely(!smap))
    {
        return -1;
    }

    arg >>= smap->cluster_bits;

    if ((arg < smap->start) || (arg > smap->end))
    {
        return -1;
    }

    return smap->spacemap_ops->unmark_smap_bit(smap, arg);
}

int mark_smap(struct spacemap * smap, UInt64 arg)
{
    if (unlikely(!smap))
    {
        return -1;
    }

    arg >>= smap->cluster_bits;

    if ((arg < smap->start) || (arg > smap->end))
    {
        return -1;
    }

    return smap->spacemap_ops->mark_smap_bit(smap, arg);
}

int unmark_smap_range(struct spacemap * smap, UInt64 block, unsigned int num)
{
    UInt64 end = block + num;

    if (unlikely(!smap))
    {
        return -1;
    }

    /* convert to clusters if necessary */
    block >>= smap->cluster_bits;
    end += (1 << smap->cluster_bits) - 1;
    end >>= smap->cluster_bits;
    num = end - block;

    if ((block < smap->start) || (block > smap->end) || (block + num - 1 > smap->end))
    {
        return -1;
    }

    return smap->spacemap_ops->unmark_smap_range(smap, block, num);
}

int mark_smap_range(struct spacemap * smap, UInt64 block, unsigned int num)
{
    UInt64 end = block + num;

    if (unlikely(!smap))
    {
        return -1;
    }
    /* convert to clusters if necessary */
    block >>= smap->cluster_bits;
    end += (1 << smap->cluster_bits) - 1;
    end >>= smap->cluster_bits;
    num = end - block;

    if ((block < smap->start) || (block > smap->end) || (block + num - 1 > smap->end))
    {
        return -1;
    }

    return smap->spacemap_ops->mark_smap_range(smap, block, num);
}

int test_smap(struct spacemap * smap, UInt64 arg)
{
    if (unlikely(!smap))
        return 0;

    arg >>= smap->cluster_bits;

    if ((arg < smap->start) || (arg > smap->end))
    {
        return -1;
    }

    return smap->spacemap_ops->test_smap_bit(smap, arg);
}

int test_smap_range(struct spacemap * smap, UInt64 block, unsigned int num)
{
    UInt64 end = block + num;

    if (unlikely(num == 1))
        return !test_smap(smap, block);

    block >>= smap->cluster_bits;
    end += (1 << smap->cluster_bits) - 1;
    end >>= smap->cluster_bits;
    num = end - block;

    if ((block < smap->start) || (block > smap->end) || (block + num - 1 > smap->end))
    {
        return -1;
    }

    return smap->spacemap_ops->test_smap_range(smap, block, num);
}

int init_space_map(struct spacemap * smap, int type, UInt64 start, UInt64 end, UInt64 real_end)
{
    int rc = 0;
    assert(smap != NULL);

    switch (type)
    {
    case SMAP64_RBTREE:
        break;
    case SMAP64_BITARRAY:
    case SMAP64_AUTODIR: // not support yet
        return -1;
    }

    smap->spacemap_ops = &smap_rbtree;
    smap->start = start;
    smap->end = end;
    smap->real_end = real_end;
    smap->cluster_bits = 0;

    rc = smap->spacemap_ops->new_smap(smap);
    if (rc != 0)
    {
        smap->spacemap_ops->free_smap(smap);
    }

    return rc;
}

void destory_space_map(struct spacemap * smap)
{
    if (smap != NULL)
        smap->spacemap_ops->free_smap(smap);
}


#ifdef __cplusplus
} // extern "C"
#endif

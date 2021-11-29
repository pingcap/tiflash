#include <Core/Types.h>
#include <common/likely.h>

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "SpaceMap.h"
#include "SpaceMapRBTree.h"

namespace DB::PS::V3 
{

void SpaceMap::printf()
{
    spacemap_ops->smapStats(this);
}

int SpaceMap::unmark(UInt64 arg)
{
    arg >>= cluster_bits;

    if ((arg < start) || (arg > end))
    {
        return -1;
    }

    return spacemap_ops->unmarkSmapBit(this, arg);
}

int SpaceMap::mark(UInt64 arg)
{
    arg >>= cluster_bits;

    if ((arg < start) || (arg > end))
    {
        return -1;
    }

    return spacemap_ops->markSmapBit(this, arg);
}

int SpaceMap::unmarkRange(UInt64 block, unsigned int num)
{
    UInt64 end = block + num;


    /* convert to clusters if necessary */
    block >>= cluster_bits;
    end += (1 << cluster_bits) - 1;
    end >>= cluster_bits;
    num = end - block;

    if ((block < start) || (block > end) || (block + num - 1 > end))
    {
        return -1;
    }

    return spacemap_ops->unmarkSmapRange(this, block, num);
}

int SpaceMap::markRange(UInt64 block, unsigned int num)
{
    UInt64 end = block + num;

    /* convert to clusters if necessary */
    block >>= cluster_bits;
    end += (1 << cluster_bits) - 1;
    end >>= cluster_bits;
    num = end - block;

    if ((block < start) || (block > end) || (block + num - 1 > end))
    {
        return -1;
    }

    return spacemap_ops->markSmapRange(this, block, num);
}

int SpaceMap::test(UInt64 arg)
{
    arg >>= cluster_bits;

    if ((arg < start) || (arg > end))
    {
        return -1;
    }

    return spacemap_ops->testSmapBit(this, arg);
}

int SpaceMap::testRange(UInt64 block, unsigned int num)
{
    UInt64 end = block + num;

    if (unlikely(num == 1))
        return !test(block);

    block >>= cluster_bits;
    end += (1 << cluster_bits) - 1;
    end >>= cluster_bits;
    num = end - block;

    if ((block < start) || (block > end) || (block + num - 1 > end))
    {
        return -1;
    }

    return spacemap_ops->testSmapRange(this, block, num);
}

SpaceMap::SpaceMap(SpaceMapOpsPtr ops, UInt64 start_, UInt64 end_, UInt64 real_end_)
    : spacemap_ops(ops),
    start(start_),
    end(end_),
    real_end(real_end_),
    cluster_bits(0)
{
    int rc = spacemap_ops->newSmap(this);
    if (rc != 0)
    {
        spacemap_ops->freeSmap(this);
    }
}

SpaceMap::~SpaceMap()
{
    spacemap_ops->freeSmap(this);
}

}

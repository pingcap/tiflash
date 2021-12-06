#include "SpaceMap.h"

#include <Core/Types.h>
#include <common/likely.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "SpaceMapRBTree.h"
#include "SpaceMapSTDMap.h"

namespace DB::PS::V3
{
SpaceMapPtr SpaceMap::createSpaceMap(SpaceMapType type, UInt64 start, UInt64 end, int cluster_bits)
{
    SpaceMapPtr smap;
    switch (type)
    {
    case SMAP64_RBTREE:
        smap = std::make_shared<RBTreeSpaceMap>(start, end, cluster_bits);
        break;
    case SMAP64_STD_MAP:
        smap = std::make_shared<STDMapSpaceMap>(start, end, cluster_bits);
        break;
    default:
        return nullptr;
    }

    int rc = smap->newSmap();
    if (rc != 0)
    {
        smap->freeSmap();
        return nullptr;
    }

    return smap;
}

std::pair<UInt64, size_t> SpaceMap::shiftBlock(UInt64 block, size_t size)
{
    UInt64 block_end = block + size;

    block >>= cluster_bits;
    block_end += (1 << cluster_bits) - 1;
    block_end >>= cluster_bits;
    size = block_end - block;
    return std::make_pair(block, size);
}


bool SpaceMap::checkRange(UInt64 block, size_t size)
{
    return (block < start) || (block > end) || (block + size - 1 > end);
}

void SpaceMap::logStats()
{
    smapStats();
}


int SpaceMap::unmarkRange(UInt64 block, size_t size)
{
    std::tie(block, size) = shiftBlock(block, size);

    if (checkRange(block, size))
    {
        LOG_ERROR(log, "unMark range out of the limit range.[type=" << type << "] [block=" << block << "], [size = " << size << "]");
        return -1;
    }

    return unmarkSmapRange(block, size);
}

int SpaceMap::markRange(UInt64 block, size_t size)
{
    std::tie(block, size) = shiftBlock(block, size);

    if (checkRange(block, size))
    {
        LOG_ERROR(log, "Mark range out of the limit range.[type=" << type << "] [block=" << block << "], [size = " << size << "]");
        return -1;
    }

    return markSmapRange(block, size);
}

int SpaceMap::testRange(UInt64 block, size_t size)
{
    std::tie(block, size) = shiftBlock(block, size);

    if (checkRange(block, size))
    {
        LOG_ERROR(log, "Test range out of the limit range.[type=" << type << "] [block=" << block << "], [size = " << size << "]");
        return -1;
    }

    return testSmapRange(block, size);
}

void SpaceMap::searchRange(size_t size, UInt64 * ret, UInt64 * max_cap)
{
    UInt64 meanless;
    UInt64 shift_cap;
    std::tie(meanless, size) = shiftBlock(0, size);

    searchSmapRange(size, ret, max_cap);
    *max_cap = *max_cap * (2 ^ cluster_bits);
}

SpaceMap::SpaceMap(UInt64 start_, UInt64 end_, int cluster_bits)
    : start(start_)
    , end(end_)
    , log(&Poco::Logger::get("RBTreeSpaceMap"))
    , cluster_bits(cluster_bits)
{
}

} // namespace DB::PS::V3

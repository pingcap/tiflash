#include "SpaceMap.h"

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <common/likely.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "SpaceMapRBTree.h"
#include "SpaceMapSTDMap.h"

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

namespace PS::V3
{
SpaceMapPtr SpaceMap::createSpaceMap(SpaceMapType type, UInt64 start, UInt64 end)
{
    SpaceMapPtr smap;
    switch (type)
    {
    case SMAP64_RBTREE:
        smap = RBTreeSpaceMap::create(start, end);
        break;
    case SMAP64_STD_MAP:
        smap = STDMapSpaceMap::create(start, end);
        break;
    default:
        throw Exception("Invalid type to create spaceMap", ErrorCodes::LOGICAL_ERROR);
    }

    if (!smap->newSmap())
    {
        smap->freeSmap();
        throw Exception("Failed create SpaceMap [type=" + typeToString(type) + "]", ErrorCodes::LOGICAL_ERROR);
    }

    return smap;
}

bool SpaceMap::checkSpace(UInt64 block, size_t size)
{
    return (block < start) || (block > end) || (block + size - 1 > end);
}

void SpaceMap::logStats()
{
    smapStats();
}

bool SpaceMap::markFree(UInt64 offset, size_t length)
{
    if (checkSpace(offset, length))
    {
        throw Exception("Unmark space out of the limit space.[type=" + typeToString(getType())
                            + "] [block=" + DB::toString(offset) + "], [size = " + DB::toString(length) + "]",
                        ErrorCodes::LOGICAL_ERROR);
    }

    return markSmapFree(offset, length);
}

bool SpaceMap::markUsed(UInt64 offset, size_t length)
{
    if (checkSpace(offset, length))
    {
        throw Exception("Mark space out of the limit space.[type=" + typeToString(getType())
                            + "] [block=" + DB::toString(offset) + "], [size = " + DB::toString(length) + "]",
                        ErrorCodes::LOGICAL_ERROR);
    }

    return markSmapUsed(offset, length);
}

bool SpaceMap::isMarkUsed(UInt64 offset, size_t length)
{
    if (checkSpace(offset, length))
    {
        throw Exception("Test space out of the limit space.[type=" + typeToString(getType())
                            + "] [block=" + DB::toString(offset) + "], [size = " + DB::toString(length) + "]",
                        ErrorCodes::LOGICAL_ERROR);
    }

    return isSmapMarkUsed(offset, length);
}

std::pair<UInt64, UInt64> SpaceMap::searchInsertOffset(size_t size)
{
    return searchSmapInsertOffset(size);
}

SpaceMap::SpaceMap(UInt64 start_, UInt64 end_, SpaceMapType type_)
    : type(type_)
    , start(start_)
    , end(end_)
    , log(&Poco::Logger::get("RBTreeSpaceMap"))
{
}

} // namespace PS::V3
} // namespace DB

// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <Storages/Page/V3/spacemap/SpaceMapSTDMap.h>
#include <common/likely.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

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
    case SMAP64_STD_MAP:
        smap = STDMapSpaceMap::create(start, end);
        break;
    default:
        throw Exception(
            fmt::format("Invalid [type={}] to create spaceMap", static_cast<UInt8>(type)),
            ErrorCodes::LOGICAL_ERROR);
    }

    if (!smap)
    {
        throw Exception(fmt::format("Failed create SpaceMap [type={}]", typeToString(type)), ErrorCodes::LOGICAL_ERROR);
    }

    return smap;
}

bool SpaceMap::checkSpace(UInt64 offset, size_t size) const
{
    return (offset < start) || (offset > end) || (offset + size - 1 > end);
}

bool SpaceMap::markFree(UInt64 offset, size_t length)
{
    if (checkSpace(offset, length))
    {
        throw Exception(
            fmt::format(
                "Unmark space out of the limit space.[type={}] [block={}], [size={}]",
                typeToString(getType()),
                offset,
                length),
            ErrorCodes::LOGICAL_ERROR);
    }

    return markFreeImpl(offset, length);
}

bool SpaceMap::markUsed(UInt64 offset, size_t length)
{
    if (checkSpace(offset, length))
    {
        throw Exception(
            fmt::format(
                "Mark space out of the limit space.[type={}] [block={}], [size={}]",
                typeToString(getType()),
                offset,
                length),
            ErrorCodes::LOGICAL_ERROR);
    }

    return markUsedImpl(offset, length);
}

bool SpaceMap::isMarkUsed(UInt64 offset, size_t length)
{
    if (checkSpace(offset, length))
    {
        throw Exception(
            fmt::format(
                "Test space out of the limit space.[type={}] [block={}], [size={}]",
                typeToString(getType()),
                offset,
                length),
            ErrorCodes::LOGICAL_ERROR);
    }

    return !isMarkUnused(offset, length);
}

SpaceMap::SpaceMap(UInt64 start_, UInt64 end_, SpaceMapType type_)
    : type(type_)
    , start(start_)
    , end(end_)
{}

} // namespace PS::V3
} // namespace DB

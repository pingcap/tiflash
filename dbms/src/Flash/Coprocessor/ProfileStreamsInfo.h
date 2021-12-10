#pragma once

#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <common/types.h>

namespace DB
{
struct ProfileStreamsInfo
{
    UInt32 qb_id = 0;
    BlockInputStreams input_streams;
};
} // namespace DB
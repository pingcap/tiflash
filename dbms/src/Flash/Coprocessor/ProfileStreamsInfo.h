#pragma once

#include <common/types.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB
{
struct ProfileStreamsInfo
{
    UInt32 qb_id;
    BlockInputStreams input_streams;
};
} // namespace DB
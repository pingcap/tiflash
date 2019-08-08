#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

class Context;

class DAGContext
{
public:
    DAGContext(size_t profile_list_size) { profile_streams_list.resize(profile_list_size); };
    std::vector<BlockInputStreams> profile_streams_list;
};
} // namespace DB

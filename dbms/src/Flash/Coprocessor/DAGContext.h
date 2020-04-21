#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

class Context;

/// A context used to track the information that needs to be passed around during DAG planning.
struct DAGContext
{
    DAGContext(){};
    std::map<String, BlockInputStreams> profile_streams_map;
};

} // namespace DB

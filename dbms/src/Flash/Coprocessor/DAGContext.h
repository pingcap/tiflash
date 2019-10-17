#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

class Context;

/// A context used to track the information that needs to be passed around during DAG planning.
struct DAGContext
{
    DAGContext(size_t profile_list_size) { profile_streams_list.resize(profile_list_size); };
    std::vector<BlockInputStreams> profile_streams_list;

    tipb::FieldType void_result_ft;
};

} // namespace DB

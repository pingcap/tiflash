#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

class Context;

struct ProfileStreamsInfo
{
    UInt32 qb_id;
    BlockInputStreams input_streams;
};
/// A context used to track the information that needs to be passed around during DAG planning.
struct DAGContext
{
    DAGContext(){};
    std::map<String, ProfileStreamsInfo> profile_streams_map;
    std::unordered_map<String, BlockInputStreams> profile_streams_map_for_join_build_side;
    std::unordered_map<UInt32, std::vector<String>> qb_id_to_join_alias_map;
    size_t final_concurency;
    Int64 compile_time_ns;
};

} // namespace DB

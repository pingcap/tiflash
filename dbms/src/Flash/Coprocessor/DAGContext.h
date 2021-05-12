#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

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
class DAGContext
{
public:
    explicit DAGContext(const tipb::DAGRequest & dag_request) : flags(dag_request.flags()), sql_mode(dag_request.sql_mode()){};
    std::map<String, ProfileStreamsInfo> & getProfileStreamsMap();
    std::unordered_map<String, BlockInputStreams> & getProfileStreamsMapForJoinBuildSide();
    std::unordered_map<UInt32, std::vector<String>> & getQBIdToJoinAliasMap();
    void handleTruncateError(const String & msg);
    void handleOverflowError(const String & msg);
    void handleDivisionByZero(const String & msg);
    void handleInvalidTime(const String & msg);
    bool allowZeroInDate() const;
    bool allowInvalidDate() const;
    bool shouldClipToZero();
    const std::vector<std::pair<Int32, String>> & getWarnings() const { return warnings; }
    
    size_t final_concurency;
    Int64 compile_time_ns;

private:
    std::map<String, ProfileStreamsInfo> profile_streams_map;
    std::unordered_map<String, BlockInputStreams> profile_streams_map_for_join_build_side;
    std::unordered_map<UInt32, std::vector<String>> qb_id_to_join_alias_map;
    std::vector<std::pair<Int32, String>> warnings;
    UInt64 flags;
    UInt64 sql_mode;
};

} // namespace DB

#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/IBlockInputStream.h>
#include <Storages/Transaction/TiDB.h>
#include <Flash/Coprocessor/DAGDriver.h>

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
    explicit DAGContext(const tipb::DAGRequest & dag_request)
        : collect_execution_summaries(dag_request.has_collect_execution_summaries() && dag_request.collect_execution_summaries()),
          return_executor_id(dag_request.has_root_executor() || dag_request.executors(0).has_executor_id()),
          is_mpp_task(false),
          is_root_mpp_task(false),
          flags(dag_request.flags()),
          sql_mode(dag_request.sql_mode()){};
    explicit DAGContext(const tipb::DAGRequest & dag_request, const mpp::TaskMeta & meta_)
        : collect_execution_summaries(dag_request.has_collect_execution_summaries() && dag_request.collect_execution_summaries()),
          return_executor_id(true),
          is_mpp_task(true),
          flags(dag_request.flags()),
          sql_mode(dag_request.sql_mode()),
          mpp_task_meta(meta_)
    {
        exchange_sender_executor_id = dag_request.root_executor().executor_id();
        const auto & exchangeSender = dag_request.root_executor().exchange_sender();
        exchange_sender_execution_summary_key = exchangeSender.child().executor_id();
        is_root_mpp_task = false;
        if (exchangeSender.encoded_task_meta_size() == 1)
        {
            /// root mpp task always has 1 task_meta because there is only one TiDB
            /// node for each mpp query
            mpp::TaskMeta task_meta;
            task_meta.ParseFromString(exchangeSender.encoded_task_meta(0));
            is_root_mpp_task = task_meta.task_id() == -1;
        }
    };
    std::map<String, ProfileStreamsInfo> & getProfileStreamsMap();
    std::unordered_map<String, BlockInputStreams> & getProfileStreamsMapForJoinBuildSide();
    std::unordered_map<UInt32, std::vector<String>> & getQBIdToJoinAliasMap();
    void handleTruncateError(const String & msg);
    void handleOverflowError(const String & msg);
    void handleDivisionByZero(const String & msg);
    void handleInvalidTime(const String & msg);
    bool shouldClipToZero();
    const std::vector<std::pair<Int32, String>> & getWarnings() const { return warnings; }
    const mpp::TaskMeta & getMPPTaskMeta() const { return mpp_task_meta; }
    bool isMPPTask() const { return is_mpp_task; }
    /// root mpp task means mpp task that send data back to TiDB
    bool isRootMPPTask() const { return is_root_mpp_task; }
    Int64 getMPPTaskId() const
    {
        if (is_mpp_task)
            return mpp_task_meta.task_id();
        return 0;
    }

    BlockInputStreams & getRemoteInputStreams() { return remote_block_input_streams; }

    size_t final_concurrency;
    Int64 compile_time_ns;
    String exchange_sender_executor_id = "";
    String exchange_sender_execution_summary_key = "";
    bool collect_execution_summaries;
    bool return_executor_id;
    bool is_mpp_task;
    bool is_root_mpp_task;

    RegionInfoList retry_regions;

private:
    /// profile_streams_map is a map that maps from executor_id to ProfileStreamsInfo
    std::map<String, ProfileStreamsInfo> profile_streams_map;
    /// profile_streams_map_for_join_build_side is a map that maps from join_build_subquery_name to
    /// the last BlockInputStreams for join build side. In TiFlash, a hash join's build side is
    /// finished before probe side starts, so the join probe side's running time does not include
    /// hash table's build time, when construct ExecSummaries, we need add the build cost to probe executor
    std::unordered_map<String, BlockInputStreams> profile_streams_map_for_join_build_side;
    /// qb_id_to_join_alias_map is a map that maps query block id to all the join_build_subquery_names
    /// in this query block and all its children query block
    std::unordered_map<UInt32, std::vector<String>> qb_id_to_join_alias_map;
    BlockInputStreams remote_block_input_streams;
    std::vector<std::pair<Int32, String>> warnings;
    UInt64 flags;
    UInt64 sql_mode;
    mpp::TaskMeta mpp_task_meta;
};

} // namespace DB

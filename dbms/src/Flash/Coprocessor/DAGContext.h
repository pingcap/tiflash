#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/LogWithPrefix.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{
class Context;
class MPPTunnelSet;

struct ProfileStreamsInfo
{
    UInt32 qb_id;
    BlockInputStreams input_streams;
};
using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;
/// A context used to track the information that needs to be passed around during DAG planning.
class DAGContext
{
public:
    explicit DAGContext(const tipb::DAGRequest & dag_request)
        : collect_execution_summaries(dag_request.has_collect_execution_summaries() && dag_request.collect_execution_summaries())
        , is_mpp_task(false)
        , is_root_mpp_task(false)
        , tunnel_set(nullptr)
        , flags(dag_request.flags())
        , sql_mode(dag_request.sql_mode())
        , warnings(std::numeric_limits<int>::max())
    {
        assert(dag_request.has_root_executor() || dag_request.executors_size() > 0);
        return_executor_id = dag_request.has_root_executor() || dag_request.executors(0).has_executor_id();
    }

    DAGContext(const tipb::DAGRequest & dag_request, const mpp::TaskMeta & meta_)
        : collect_execution_summaries(dag_request.has_collect_execution_summaries() && dag_request.collect_execution_summaries())
        , return_executor_id(true)
        , is_mpp_task(true)
        , tunnel_set(nullptr)
        , flags(dag_request.flags())
        , sql_mode(dag_request.sql_mode())
        , mpp_task_meta(meta_)
        , warnings(std::numeric_limits<int>::max())
    {
        assert(dag_request.has_root_executor());

        exchange_sender_executor_id = dag_request.root_executor().executor_id();
        const auto & exchange_sender = dag_request.root_executor().exchange_sender();
        exchange_sender_execution_summary_key = exchange_sender.child().executor_id();
        is_root_mpp_task = false;
        if (exchange_sender.encoded_task_meta_size() == 1)
        {
            /// root mpp task always has 1 task_meta because there is only one TiDB
            /// node for each mpp query
            mpp::TaskMeta task_meta;
            task_meta.ParseFromString(exchange_sender.encoded_task_meta(0));
            is_root_mpp_task = task_meta.task_id() == -1;
        }
    }

    DAGContext()
        : collect_execution_summaries(false)
        , is_mpp_task(false)
        , is_root_mpp_task(false)
        , tunnel_set(nullptr)
        , flags(0)
        , sql_mode(0)
        , warnings(std::numeric_limits<int>::max())
    {}

    std::map<String, ProfileStreamsInfo> & getProfileStreamsMap();
    std::unordered_map<String, BlockInputStreams> & getProfileStreamsMapForJoinBuildSide();
    std::unordered_map<UInt32, std::vector<String>> & getQBIdToJoinAliasMap();
    void handleTruncateError(const String & msg);
    void handleUnknownLocale(const String & value);
    void handleOverflowError(const String & msg, const TiFlashError & error);
    void handleDivisionByZero();
    void handleInvalidTime(const String & msg, const TiFlashError & error);
    bool allowZeroInDate() const;
    bool allowInvalidDate() const;
    bool shouldClipToZero();
    /// This method is thread-safe.
    void appendWarning(const tipb::Error & warning)
    {
        if (!warnings.tryPush(warning))
            throw TiFlashException("Too many warnings, exceeds limit of 2147483647", Errors::Coprocessor::Internal);
    }
    /// Consume all warnings. Once this method called, every warning will be cleared.
    /// This method is not thread-safe.
    void consumeWarnings(std::vector<tipb::Error> & warnings_)
    {
        const size_t warnings_size = warnings.size();
        warnings_.reserve(warnings_size);
        for (size_t i = 0; i < warnings_size; ++i)
        {
            tipb::Error error;
            warnings.pop(error);
            warnings_.push_back(error);
        }
    }
    void clearWarnings() { warnings.clear(); }
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

    std::pair<bool, double> getTableScanThroughput();

    size_t final_concurrency;
    Int64 compile_time_ns;
    String table_scan_executor_id = "";
    String exchange_sender_executor_id = "";
    String exchange_sender_execution_summary_key = "";
    bool collect_execution_summaries;
    bool return_executor_id;
    bool is_mpp_task;
    bool is_root_mpp_task;
    MPPTunnelSetPtr tunnel_set;
    RegionInfoList retry_regions;

    LogWithPrefixPtr mpp_task_log;

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
    UInt64 flags;
    UInt64 sql_mode;
    mpp::TaskMeta mpp_task_meta;
    ConcurrentBoundedQueue<tipb::Error> warnings;
};

} // namespace DB

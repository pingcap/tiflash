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
#include <Flash/Mpp/MPPTaskId.h>
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

class Join;
using JoinPtr = std::shared_ptr<Join>;
struct JoinExecuteInfo
{
    String build_side_root_executor_id;
    JoinPtr join_ptr;
    BlockInputStreams non_joined_streams;
};

using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;

UInt64 inline getMaxErrorCount(const tipb::DAGRequest &)
{
    /// todo max_error_count is a system variable in mysql, TiDB should put it into dag request, now use the default value instead
    return 1024;
}

class ExchangeReceiver;

/// A context used to track the information that needs to be passed around during DAG planning.
class DAGContext
{
public:
    explicit DAGContext(const tipb::DAGRequest & dag_request_)
        : dag_request(&dag_request_)
        , collect_execution_summaries(dag_request->has_collect_execution_summaries() && dag_request->collect_execution_summaries())
        , is_mpp_task(false)
        , is_root_mpp_task(false)
        , tunnel_set(nullptr)
        , flags(dag_request->flags())
        , sql_mode(dag_request->sql_mode())
        , max_recorded_error_count(getMaxErrorCount(*dag_request))
        , warnings(max_recorded_error_count)
        , warning_count(0)
    {
        assert(dag_request->has_root_executor() || dag_request->executors_size() > 0);
        return_executor_id = dag_request->root_executor().has_executor_id() || dag_request->executors(0).has_executor_id();
    }

    DAGContext(const tipb::DAGRequest & dag_request_, const mpp::TaskMeta & meta_, bool is_root_mpp_task_)
        : dag_request(&dag_request_)
        , collect_execution_summaries(dag_request->has_collect_execution_summaries() && dag_request->collect_execution_summaries())
        , return_executor_id(true)
        , is_mpp_task(true)
        , is_root_mpp_task(is_root_mpp_task_)
        , tunnel_set(nullptr)
        , flags(dag_request->flags())
        , sql_mode(dag_request->sql_mode())
        , mpp_task_meta(meta_)
        , mpp_task_id(mpp_task_meta.start_ts(), mpp_task_meta.task_id())
        , max_recorded_error_count(getMaxErrorCount(*dag_request))
        , warnings(max_recorded_error_count)
        , warning_count(0)
    {
        assert(dag_request->has_root_executor() && dag_request->root_executor().has_executor_id());
    }

    explicit DAGContext(UInt64 max_error_count_)
        : dag_request(nullptr)
        , collect_execution_summaries(false)
        , is_mpp_task(false)
        , is_root_mpp_task(false)
        , tunnel_set(nullptr)
        , flags(0)
        , sql_mode(0)
        , max_recorded_error_count(max_error_count_)
        , warnings(max_recorded_error_count)
        , warning_count(0)
    {}

    void attachBlockIO(const BlockIO & io_);
    std::map<String, ProfileStreamsInfo> & getProfileStreamsMap();
    std::unordered_map<String, BlockInputStreams> & getProfileStreamsMapForJoinBuildSide();
    std::unordered_map<UInt32, std::vector<String>> & getQBIdToJoinAliasMap();
    std::unordered_map<String, JoinExecuteInfo> & getJoinExecuteInfoMap();
    std::unordered_map<String, BlockInputStreams> & getInBoundIOInputStreamsMap();
    void handleTruncateError(const String & msg);
    void handleOverflowError(const String & msg, const TiFlashError & error);
    void handleDivisionByZero();
    void handleInvalidTime(const String & msg, const TiFlashError & error);
    void appendWarning(const String & msg, int32_t code = 0);
    bool allowZeroInDate() const;
    bool allowInvalidDate() const;
    bool shouldClipToZero();
    /// This method is thread-safe.
    void appendWarning(const tipb::Error & warning)
    {
        if (warning_count.fetch_add(1, std::memory_order_acq_rel) < max_recorded_error_count)
        {
            warnings.tryPush(warning);
        }
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
    UInt64 getWarningCount() { return warning_count; }
    const mpp::TaskMeta & getMPPTaskMeta() const { return mpp_task_meta; }
    bool isBatchCop() const { return is_batch_cop; }
    bool isMPPTask() const { return is_mpp_task; }
    /// root mpp task means mpp task that send data back to TiDB
    bool isRootMPPTask() const { return is_root_mpp_task; }
    const MPPTaskId & getMPPTaskId() const
    {
        return mpp_task_id;
    }

    std::pair<bool, double> getTableScanThroughput();

    const RegionInfoMap & getRegionsForLocalRead() const { return regions_for_local_read; }
    const RegionInfoList & getRegionsForRemoteRead() const { return regions_for_remote_read; }

    const BlockIO & getBlockIO() const
    {
        return io;
    }

    std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & getMPPExchangeReceiverMapRef()
    {
        return mpp_exchange_receiver_map;
    }

    void cancelAllExchangeReceiver();

    const tipb::DAGRequest * dag_request;
    Int64 compile_time_ns = 0;
    size_t final_concurrency = 1;
    bool has_read_wait_index = false;
    Clock::time_point read_wait_index_start_timestamp{Clock::duration::zero()};
    Clock::time_point read_wait_index_end_timestamp{Clock::duration::zero()};
    String table_scan_executor_id = "";
    String tidb_host = "Unknown";
    bool collect_execution_summaries;
    bool return_executor_id;
    bool is_mpp_task = false;
    bool is_root_mpp_task = false;
    bool is_batch_cop = false;
    MPPTunnelSetPtr tunnel_set;
    RegionInfoMap regions_for_local_read;
    RegionInfoList regions_for_remote_read;
    // part of regions_for_local_read + regions_for_remote_read, only used for batch-cop
    RegionInfoList retry_regions;

    LogWithPrefixPtr log;

    bool keep_session_timezone_info = false;
    std::vector<tipb::FieldType> result_field_types;
    tipb::EncodeType encode_type = tipb::EncodeType::TypeDefault;

private:
    /// Hold io for correcting the destruction order.
    BlockIO io;
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
    /// join_execute_info_map is a map that maps from join_probe_executor_id to JoinExecuteInfo
    /// JoinStatistics gets JoinExecuteInfo through it.
    std::unordered_map<std::string, JoinExecuteInfo> join_execute_info_map;
    /// profile_streams_map is a map that maps from executor_id (table_scan / exchange_receiver) to BlockInputStreams.
    /// BlockInputStreams contains ExchangeReceiverInputStream, CoprocessorBlockInputStream and local_read_input_stream etc.
    std::unordered_map<String, BlockInputStreams> inbound_io_input_streams_map;
    UInt64 flags;
    UInt64 sql_mode;
    mpp::TaskMeta mpp_task_meta;
    const MPPTaskId mpp_task_id = MPPTaskId::unknown_mpp_task_id;
    /// max_recorded_error_count is the max error/warning need to be recorded in warnings
    UInt64 max_recorded_error_count;
    ConcurrentBoundedQueue<tipb::Error> warnings;
    /// warning_count is the actual warning count during the entire execution
    std::atomic<UInt64> warning_count;
    // mpp_exchange_receiver_map holds exchange_receivers
    std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> mpp_exchange_receiver_map;
};

} // namespace DB

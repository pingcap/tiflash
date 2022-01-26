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

UInt64 inline getMaxErrorCount(const tipb::DAGRequest &)
{
    /// todo max_error_count is a system variable in mysql, TiDB should put it into dag request, now use the default value instead
    return 1024;
}
<<<<<<< HEAD
=======

namespace TiDBSQLFlags
{
constexpr UInt64 IGNORE_TRUNCATE = 1;
constexpr UInt64 TRUNCATE_AS_WARNING = 1u << 1u;
constexpr UInt64 PAD_CHAR_TO_FULL_LENGTH = 1u << 2u;
constexpr UInt64 IN_INSERT_STMT = 1u << 3u;
constexpr UInt64 IN_UPDATE_OR_DELETE_STMT = 1u << 4u;
constexpr UInt64 IN_SELECT_STMT = 1u << 5u;
constexpr UInt64 OVERFLOW_AS_WARNING = 1u << 6u;
constexpr UInt64 IGNORE_ZERO_IN_DATE = 1u << 7u;
constexpr UInt64 DIVIDED_BY_ZERO_AS_WARNING = 1u << 8u;
constexpr UInt64 IN_LOAD_DATA_STMT = 1u << 10u;
} // namespace TiDBSQLFlags

namespace TiDBSQLMode
{
constexpr UInt64 REAL_AS_FLOAT = 1ul;
constexpr UInt64 PIPES_AS_CONCAT = 1ul << 1ul;
constexpr UInt64 ANSI_QUOTES = 1ul << 2ul;
constexpr UInt64 IGNORE_SPACE = 1ul << 3ul;
constexpr UInt64 NOT_USED = 1ul << 4ul;
constexpr UInt64 ONLY_FULL_GROUP_BY = 1ul << 5ul;
constexpr UInt64 NO_UNSIGNED_SUBTRACTION = 1ul << 6ul;
constexpr UInt64 NO_DIR_IN_CREATE = 1ul << 7ul;
constexpr UInt64 POSTGRESQL = 1ul << 8ul;
constexpr UInt64 ORACLE = 1ul << 9ul;
constexpr UInt64 MSSQL = 1ul << 10ul;
constexpr UInt64 DB2 = 1ul << 11ul;
constexpr UInt64 MAXDB = 1ul << 12ul;
constexpr UInt64 NO_KEY_OPTIONS = 1ul << 13ul;
constexpr UInt64 NO_TABLE_OPTIONS = 1ul << 14ul;
constexpr UInt64 NO_FIELD_OPTIONS = 1ul << 15ul;
constexpr UInt64 MYSQL323 = 1ul << 16ul;
constexpr UInt64 MYSQL40 = 1ul << 17ul;
constexpr UInt64 ANSI = 1ul << 18ul;
constexpr UInt64 NO_AUTO_VALUE_ON_ZERO = 1ul << 19ul;
constexpr UInt64 NO_BACK_SLASH_ESCAPES = 1ul << 20ul;
constexpr UInt64 STRICT_TRANS_TABLES = 1ul << 21ul;
constexpr UInt64 STRICT_ALL_TABLES = 1ul << 22ul;
constexpr UInt64 NO_ZERO_IN_DATE = 1ul << 23ul;
constexpr UInt64 NO_ZERO_DATE = 1ul << 24ul;
constexpr UInt64 INVALID_DATES = 1ul << 25ul;
constexpr UInt64 ERROR_FOR_DIVISION_BY_ZERO = 1ul << 26ul;
constexpr UInt64 TRADITIONAL = 1ul << 27ul;
constexpr UInt64 NO_AUTO_CREATE_USER = 1ul << 28ul;
constexpr UInt64 HIGH_NOT_PRECEDENCE = 1ul << 29ul;
constexpr UInt64 NO_ENGINE_SUBSTITUTION = 1ul << 30ul;

// Duplicated with Flag::PAD_CHAR_TO_FULL_LENGTH
// PAD_CHAR_TO_FULL_LENGTH = 1ul << 31ul;

constexpr UInt64 ALLOW_INVALID_DATES = 1ul << 32ul;
} // namespace TiDBSQLMode

>>>>>>> 6ea6c80198 (Fix cast to decimal overflow bug (#3922))
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
        , max_recorded_error_count(getMaxErrorCount(dag_request))
        , warnings(max_recorded_error_count)
        , warning_count(0)
    {
        assert(dag_request.has_root_executor() || dag_request.executors_size() > 0);
        return_executor_id = dag_request.has_root_executor() || dag_request.executors(0).has_executor_id();
    }

    DAGContext(const tipb::DAGRequest & dag_request, const mpp::TaskMeta & meta_, bool is_root_mpp_task_)
        : collect_execution_summaries(dag_request.has_collect_execution_summaries() && dag_request.collect_execution_summaries())
        , return_executor_id(true)
        , is_mpp_task(true)
        , is_root_mpp_task(is_root_mpp_task_)
        , tunnel_set(nullptr)
        , flags(dag_request.flags())
        , sql_mode(dag_request.sql_mode())
        , mpp_task_meta(meta_)
        , max_recorded_error_count(getMaxErrorCount(dag_request))
        , warnings(max_recorded_error_count)
        , warning_count(0)
    {
        assert(dag_request.has_root_executor());
        exchange_sender_executor_id = dag_request.root_executor().executor_id();
        exchange_sender_execution_summary_key = dag_request.root_executor().exchange_sender().child().executor_id();
    }

    explicit DAGContext(UInt64 max_error_count_)
        : collect_execution_summaries(false)
        , is_mpp_task(false)
        , is_root_mpp_task(false)
        , tunnel_set(nullptr)
        , flags(0)
        , sql_mode(0)
        , max_recorded_error_count(max_error_count_)
        , warnings(max_recorded_error_count)
        , warning_count(0)
    {}

    std::map<String, ProfileStreamsInfo> & getProfileStreamsMap();
    std::unordered_map<String, BlockInputStreams> & getProfileStreamsMapForJoinBuildSide();
    std::unordered_map<UInt32, std::vector<String>> & getQBIdToJoinAliasMap();
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

<<<<<<< HEAD
    std::pair<bool, double> getTableScanThroughput();
=======
    UInt64 getFlags() const
    {
        return flags;
    }
    void setFlags(UInt64 f)
    {
        flags = f;
    }
    void addFlag(UInt64 f)
    {
        flags |= f;
    }
    void delFlag(UInt64 f)
    {
        flags &= (~f);
    }
    bool hasFlag(UInt64 f) const
    {
        return (flags & f);
    }

    void initExchangeReceiverIfMPP(Context & context, size_t max_streams);
    const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & getMPPExchangeReceiverMap() const;
>>>>>>> 6ea6c80198 (Fix cast to decimal overflow bug (#3922))

    size_t final_concurrency = 1;
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
    /// max_recorded_error_count is the max error/warning need to be recorded in warnings
    UInt64 max_recorded_error_count;
    ConcurrentBoundedQueue<tipb::Error> warnings;
    /// warning_count is the actual warning count during the entire execution
    std::atomic<UInt64> warning_count;
};

} // namespace DB

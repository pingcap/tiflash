// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <kvproto/mpp.pb.h>
#pragma GCC diagnostic pop

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Logger.h>
#include <Core/QueryOperatorSpillContexts.h>
#include <Core/TaskOperatorSpillContexts.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/DAGRequest.h>
#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Coprocessor/RuntimeFilterMgr.h>
#include <Flash/Coprocessor/TablesRegionsInfo.h>
#include <Flash/Executor/toRU.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Interpreters/SubqueryForSet.h>
#include <Operators/IOProfileInfo.h>
#include <Operators/OperatorProfileInfo.h>
#include <Parsers/makeDummyQuery.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>

namespace DB
{
class Context;
class MPPTunnelSet;
class ExchangeReceiver;
using ExchangeReceiverPtr = std::shared_ptr<ExchangeReceiver>;
/// key: executor_id of ExchangeReceiver nodes in dag.
using ExchangeReceiverMap = std::unordered_map<String, ExchangeReceiverPtr>;
class MPPReceiverSet;
using MPPReceiverSetPtr = std::shared_ptr<MPPReceiverSet>;
class CoprocessorReader;
using CoprocessorReaderPtr = std::shared_ptr<CoprocessorReader>;

class AutoSpillTrigger;

struct JoinProfileInfo;
using JoinProfileInfoPtr = std::shared_ptr<JoinProfileInfo>;
struct JoinExecuteInfo
{
    String build_side_root_executor_id;
    JoinProfileInfoPtr join_profile_info;
    BlockInputStreams join_build_streams;
    OperatorProfileInfos join_build_profile_infos;
};

using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;

class ProcessListEntry;

UInt64 inline getMaxErrorCount(const tipb::DAGRequest &)
{
    /// todo max_error_count is a system variable in mysql, TiDB should put it into dag request, now use the default value instead
    return 1024;
}

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

constexpr Int32 DEFAULT_DIV_PRECISION_INCREMENT = 4;

enum class ExecutionMode
{
    None,
    Stream,
    Pipeline,
};

enum class DAGRequestKind
{
    Cop,
    CopStream,
    BatchCop,
    MPP,
};

/// A context used to track the information that needs to be passed around during DAG planning.
class DAGContext
{
public:
    // for non-mpp(Cop/CopStream/BatchCop)
    DAGContext(
        tipb::DAGRequest & dag_request_,
        TablesRegionsInfo && tables_regions_info_,
        KeyspaceID keyspace_id_,
        const String & tidb_host_,
        DAGRequestKind cop_kind_,
        const String & resource_group_name,
        UInt64 connection_id_,
        const String & connection_alias_,
        LoggerPtr log_);

    // for mpp
    DAGContext(tipb::DAGRequest & dag_request_, const mpp::TaskMeta & meta_, bool is_root_mpp_task_);

    // for disaggregated task on write node
    DAGContext(
        tipb::DAGRequest & dag_request_,
        const disaggregated::DisaggTaskMeta & task_meta_,
        TablesRegionsInfo && tables_regions_info_,
        const String & compute_node_host_,
        LoggerPtr log_);

    // for test
    explicit DAGContext(UInt64 max_error_count_);

    // for tests need to run query tasks.
    DAGContext(tipb::DAGRequest & dag_request_, String log_identifier, size_t concurrency);

    ~DAGContext();

    std::unordered_map<String, BlockInputStreams> & getProfileStreamsMap();

    std::unordered_map<String, OperatorProfileInfos> & getOperatorProfileInfosMap();

    void addOperatorProfileInfos(
        const String & executor_id,
        OperatorProfileInfos && profile_infos,
        bool is_append = false);

    std::unordered_map<String, std::vector<String>> & getExecutorIdToJoinIdMap();

    std::unordered_map<String, JoinExecuteInfo> & getJoinExecuteInfoMap();

    std::unordered_map<String, BlockInputStreams> & getInBoundIOInputStreamsMap();

    std::unordered_map<String, IOProfileInfos> & getInboundIOProfileInfosMap();

    void addInboundIOProfileInfos(
        const String & executor_id,
        IOProfileInfos && io_profile_infos,
        bool is_append = false);

    void handleTruncateError(const String & msg);
    void handleOverflowError(const String & msg, const TiFlashError & error);
    void handleDivisionByZero();
    void handleInvalidTime(const String & msg, const TiFlashError & error);
    void appendWarning(const String & msg, int32_t code = 0);
    bool allowZeroInDate() const;
    bool allowInvalidDate() const;
    bool shouldClipToZero() const;
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
    void fillWarnings(tipb::SelectResponse & response)
    {
        std::vector<tipb::Error> warnings_vec;
        consumeWarnings(warnings_vec);
        for (auto & warning : warnings_vec)
        {
            auto * warn = response.add_warnings();
            // TODO: consider using allocated warnings to prevent copy?
            warn->CopyFrom(warning);
        }
        response.set_warning_count(getWarningCount());
    }
    void clearWarnings()
    {
        warnings.clear();
        warning_count = 0;
    }
    UInt64 getWarningCount() { return warning_count; }
    const mpp::TaskMeta & getMPPTaskMeta() const { return mpp_task_meta; }
    bool isCop() const { return kind == DAGRequestKind::Cop; }
    bool isCopStream() const { return kind == DAGRequestKind::CopStream; }
    bool isBatchCop() const { return kind == DAGRequestKind::BatchCop; }
    bool isMPPTask() const { return kind == DAGRequestKind::MPP; }
    /// root mpp task means mpp task that send data back to TiDB
    bool isRootMPPTask() const { return is_root_mpp_task; }
    const MPPTaskId & getMPPTaskId() const { return mpp_task_id; }
    const std::unique_ptr<DM::DisaggTaskId> & getDisaggTaskId() const { return disaggregated_id; }

    std::pair<bool, double> getTableScanThroughput();

    const SingleTableRegions & getTableRegionsInfoByTableID(Int64 table_id) const;

    bool containsRegionsInfoForTable(Int64 table_id) const;

    UInt64 getFlags() const { return flags; }
    void setFlags(UInt64 f) { flags = f; }
    void addFlag(UInt64 f) { flags |= f; }
    void delFlag(UInt64 f) { flags &= (~f); }
    bool hasFlag(UInt64 f) const { return (flags & f); }

    UInt64 getSQLMode() const { return sql_mode; }
    void setSQLMode(UInt64 f) { sql_mode = f; }
    void addSQLMode(UInt64 f) { sql_mode |= f; }
    void delSQLMode(UInt64 f) { sql_mode &= (~f); }
    bool hasSQLMode(UInt64 f) const { return sql_mode & f; }

    Int32 getDivPrecisionIncrement() const { return div_precision_increment; }
    // for test usage only
    void setDivPrecisionIncrement(Int32 new_value) { div_precision_increment = new_value; }

    void updateFinalConcurrency(size_t cur_streams_size, size_t streams_upper_limit);

    ExchangeReceiverPtr getMPPExchangeReceiver(const String & executor_id) const;
    void setMPPReceiverSet(const MPPReceiverSetPtr & receiver_set) { mpp_receiver_set = receiver_set; }
    void addCoprocessorReader(const CoprocessorReaderPtr & coprocessor_reader);
    std::vector<CoprocessorReaderPtr> & getCoprocessorReaders();

    void addSubquery(const String & subquery_id, SubqueryForSet && subquery);
    bool hasSubquery() const { return !subqueries.empty(); }
    std::vector<SubqueriesForSets> && moveSubqueries() { return std::move(subqueries); }
    void setProcessListEntry(const std::shared_ptr<ProcessListEntry> & entry) { process_list_entry = entry; }
    std::shared_ptr<ProcessListEntry> getProcessListEntry() const { return process_list_entry; }
    void setQueryOperatorSpillContexts(
        const std::shared_ptr<QueryOperatorSpillContexts> & query_operator_spill_contexts_)
    {
        query_operator_spill_contexts = query_operator_spill_contexts_;
    }
    std::shared_ptr<QueryOperatorSpillContexts> & getQueryOperatorSpillContexts()
    {
        return query_operator_spill_contexts;
    }
    void setAutoSpillTrigger(const std::shared_ptr<AutoSpillTrigger> & auto_spill_trigger_)
    {
        auto_spill_trigger = auto_spill_trigger_;
    }
    AutoSpillTrigger * getAutoSpillTrigger()
    {
        return auto_spill_trigger == nullptr ? nullptr : auto_spill_trigger.get();
    }

    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }

    KeyspaceID getKeyspaceID() const { return keyspace_id; }
    String getResourceGroupName() { return resource_group_name; }
    // For now, only called for BlockIO execution engine to disable report RU of storage layer.
    void clearResourceGroupName() { resource_group_name = ""; }

    UInt64 getReadBytes() const;

    void switchToStreamMode()
    {
        RUNTIME_CHECK(execution_mode == ExecutionMode::None);
        execution_mode = ExecutionMode::Stream;
    }
    void switchToPipelineMode()
    {
        RUNTIME_CHECK(execution_mode == ExecutionMode::None);
        execution_mode = ExecutionMode::Pipeline;
    }
    ExecutionMode getExecutionMode() const { return execution_mode; }

    void registerOperatorSpillContext(const OperatorSpillContextPtr & operator_spill_context)
    {
        if (in_auto_spill_mode)
            operator_spill_contexts->registerOperatorSpillContext(operator_spill_context);
    }

    void registerTaskOperatorSpillContexts()
    {
        query_operator_spill_contexts->registerTaskOperatorSpillContexts(operator_spill_contexts);
    }

    void setAutoSpillMode() { in_auto_spill_mode = true; }
    bool isInAutoSpillMode() const { return in_auto_spill_mode; }

    UInt64 getConnectionID() const { return connection_id; }
    const String & getConnectionAlias() const { return connection_alias; }

    MPPReceiverSetPtr getMPPReceiverSet() const { return mpp_receiver_set; }

public:
    DAGRequest dag_request;
    /// Some existing code inherited from Clickhouse assume that each query must have a valid query string and query ast,
    /// dummy_query_string and dummy_ast is used for that
    String dummy_query_string;
    ASTPtr dummy_ast;
    Int64 compile_time_ns = 0;
    Int64 minTSO_wait_time_ns = 0;
    size_t final_concurrency = 1;
    size_t initialize_concurrency = 1;
    bool has_read_wait_index = false;
    Clock::time_point read_wait_index_start_timestamp{Clock::duration::zero()};
    Clock::time_point read_wait_index_end_timestamp{Clock::duration::zero()};
    String table_scan_executor_id;

    // For mpp/cop/batchcop this is the host of tidb
    // For disaggregated read, this is the host of compute node
    String tidb_host = "Unknown";
    bool collect_execution_summaries{};
    /* const */ DAGRequestKind kind;
    /* const */ bool is_root_mpp_task = false;
    /* const */ bool is_disaggregated_task = false; // a disagg task handling by the write node
    // `tunnel_set` is always set by `MPPTask` and is used later.
    MPPTunnelSetPtr tunnel_set;
    TablesRegionsInfo tables_regions_info;
    // part of regions_for_local_read + regions_for_remote_read, only used for batch-cop
    RegionInfoList retry_regions;

    LoggerPtr log;

    // initialized in `initOutputInfo`.
    std::vector<tipb::FieldType> result_field_types;
    tipb::EncodeType encode_type = tipb::EncodeType::TypeDefault;
    // only meaningful in final projection.
    bool keep_session_timezone_info = false;
    std::vector<tipb::FieldType> output_field_types;
    std::vector<Int32> output_offsets;

    /// executor_id, ScanContextPtr
    /// Currently, max(scan_context_map.size()) == 1, because one mpp task only have do one table scan
    /// While when we support collcate join later, scan_context_map.size() may > 1,
    /// thus we need to pay attention to scan_context_map usage that time.
    std::unordered_map<String, DM::ScanContextPtr> scan_context_map;

    RuntimeFilterMgr runtime_filter_mgr;

private:
    void initExecutorIdToJoinIdMap();
    void initOutputInfo();
    tipb::EncodeType analyzeDAGEncodeType() const;

private:
    std::shared_ptr<ProcessListEntry> process_list_entry;
    bool in_auto_spill_mode = false;
    std::shared_ptr<TaskOperatorSpillContexts> operator_spill_contexts;
    std::shared_ptr<QueryOperatorSpillContexts> query_operator_spill_contexts;
    std::shared_ptr<AutoSpillTrigger> auto_spill_trigger;
    /// Holding the table lock to make sure that the table wouldn't be dropped during the lifetime of this query, even if there are no local regions.
    /// TableLockHolders need to be released after the BlockInputStream is destroyed to prevent data read exceptions.
    TableLockHolders table_locks;

    /// operator profile related
    /// operator_profile_infos will be added to map concurrently at runtime, so a lock is needed to prevent data race.
    std::mutex operator_profile_infos_map_mu;
    /// profile_streams_map is a map that maps from executor_id to profile BlockInputStreams.
    std::unordered_map<String, BlockInputStreams> profile_streams_map;
    /// operator_profile_infos_map is a map that maps from executor_id to OperatorProfileInfos.
    std::unordered_map<String, OperatorProfileInfos> operator_profile_infos_map;
    /// executor_id_to_join_id_map is a map that maps executor id to all the join executor id of itself and all its children.
    std::unordered_map<String, std::vector<String>> executor_id_to_join_id_map;
    /// join_execute_info_map is a map that maps from join_probe_executor_id to JoinExecuteInfo
    /// DAGResponseWriter / JoinStatistics gets JoinExecuteInfo through it.
    std::unordered_map<std::string, JoinExecuteInfo> join_execute_info_map;
    /// inbound_io_input_streams_map is a map that maps from executor_id (table_scan / exchange_receiver) to BlockInputStreams.
    /// BlockInputStreams contains ExchangeReceiverInputStream, CoprocessorBlockInputStream and local_read_input_stream etc.
    std::unordered_map<String, BlockInputStreams> inbound_io_input_streams_map;
    /// inbound_io_profile_infos_map is a map that maps from executor_id (table_scan / exchange_receiver) to IOProfileInfos.
    /// IOProfileInfos are from ExchangeReceiverSourceOp, CoprocessorSourceOp and local_read_source etc.
    std::unordered_map<String, IOProfileInfos> inbound_io_profile_infos_map;

    UInt64 flags;
    UInt64 sql_mode;
    Int32 div_precision_increment = DEFAULT_DIV_PRECISION_INCREMENT;
    mpp::TaskMeta mpp_task_meta;
    const MPPTaskId mpp_task_id = MPPTaskId::unknown_mpp_task_id;
    // The task id for disaggregated read
    const std::unique_ptr<DM::DisaggTaskId> disaggregated_id;
    /// max_recorded_error_count is the max error/warning need to be recorded in warnings
    UInt64 max_recorded_error_count;
    ConcurrentBoundedQueue<tipb::Error> warnings;
    /// warning_count is the actual warning count during the entire execution
    std::atomic<UInt64> warning_count;

    // `mpp_receiver_set` is always set by `MPPTask` and is used later.
    MPPReceiverSetPtr mpp_receiver_set;
    std::vector<CoprocessorReaderPtr> coprocessor_readers;
    /// vector of SubqueriesForSets(such as join build subquery).
    /// The order of the vector is also the order of the subquery.
    std::vector<SubqueriesForSets> subqueries;

    // The keyspace that the DAG request from
    const KeyspaceID keyspace_id = NullspaceID;

    String resource_group_name;

    // Used to determine the execution mode
    // - None: request has not been executed yet
    // - Stream: execute with block input stream
    // - Pipeline: execute with pipeline model
    ExecutionMode execution_mode = ExecutionMode::None;

    // It's the session id between mysql client and tidb
    UInt64 connection_id;
    // It's the session alias between mysql client and tidb
    String connection_alias;
};

} // namespace DB

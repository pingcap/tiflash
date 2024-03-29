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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Flash/Coprocessor/collectOutputFieldTypes.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Statistics/transformProfiles.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/KVStore/TMTContext.h>
#include <kvproto/disaggregated.pb.h>
#include <tipb/executor.pb.h>

#include <mutex>

namespace DB
{
namespace ErrorCodes
{
extern const int TRUNCATE_ERROR;
extern const int OVERFLOW_ERROR;
extern const int DIVIDED_BY_ZERO;
extern const int INVALID_TIME;
} // namespace ErrorCodes

namespace
{
bool strictSqlMode(UInt64 sql_mode)
{
    return sql_mode & TiDBSQLMode::STRICT_ALL_TABLES || sql_mode & TiDBSQLMode::STRICT_TRANS_TABLES;
}
} // namespace

// for non-mpp(Cop/CopStream/BatchCop)
DAGContext::DAGContext(
    tipb::DAGRequest & dag_request_,
    TablesRegionsInfo && tables_regions_info_,
    KeyspaceID keyspace_id_,
    const String & tidb_host_,
    DAGRequestKind kind_,
    const String & resource_group_name_,
    UInt64 connection_id_,
    const String & connection_alias_,
    LoggerPtr log_)
    : dag_request(&dag_request_)
    , dummy_query_string(dag_request->ShortDebugString())
    , dummy_ast(makeDummyQuery())
    , tidb_host(tidb_host_)
    , collect_execution_summaries(
          dag_request->has_collect_execution_summaries() && dag_request->collect_execution_summaries())
    , kind(kind_)
    , is_root_mpp_task(false)
    , tables_regions_info(std::move(tables_regions_info_))
    , log(std::move(log_))
    , operator_spill_contexts(std::make_shared<TaskOperatorSpillContexts>())
    , flags(dag_request->flags())
    , sql_mode(dag_request->sql_mode())
    , max_recorded_error_count(getMaxErrorCount(*dag_request))
    , warnings(max_recorded_error_count)
    , warning_count(0)
    , keyspace_id(keyspace_id_)
    , resource_group_name(resource_group_name_)
    , connection_id(connection_id_)
    , connection_alias(connection_alias_)
{
    RUNTIME_ASSERT(kind != DAGRequestKind::MPP, log, "DAGContext non-mpp constructor get a mpp kind");
    if (dag_request->has_div_precision_increment())
        div_precision_increment = dag_request->div_precision_increment();
    initOutputInfo();
}

// for mpp
DAGContext::DAGContext(tipb::DAGRequest & dag_request_, const mpp::TaskMeta & meta_, bool is_root_mpp_task_)
    : dag_request(&dag_request_)
    , dummy_query_string(dag_request->ShortDebugString())
    , dummy_ast(makeDummyQuery())
    , collect_execution_summaries(
          dag_request->has_collect_execution_summaries() && dag_request->collect_execution_summaries())
    , kind(DAGRequestKind::MPP)
    , is_root_mpp_task(is_root_mpp_task_)
    , operator_spill_contexts(std::make_shared<TaskOperatorSpillContexts>())
    , flags(dag_request->flags())
    , sql_mode(dag_request->sql_mode())
    , mpp_task_meta(meta_)
    , mpp_task_id(mpp_task_meta)
    , max_recorded_error_count(getMaxErrorCount(*dag_request))
    , warnings(max_recorded_error_count)
    , warning_count(0)
    , keyspace_id(RequestUtils::deriveKeyspaceID(meta_))
    , resource_group_name(meta_.resource_group_name())
    , connection_id(meta_.connection_id())
    , connection_alias(meta_.connection_alias())
{
    if (dag_request->has_div_precision_increment())
        div_precision_increment = dag_request->div_precision_increment();
    // only mpp task has join executor.
    initExecutorIdToJoinIdMap();
    initOutputInfo();
}

// for disaggregated task on write node
DAGContext::DAGContext(
    tipb::DAGRequest & dag_request_,
    const disaggregated::DisaggTaskMeta & task_meta_,
    TablesRegionsInfo && tables_regions_info_,
    const String & compute_node_host_,
    LoggerPtr log_)
    : dag_request(&dag_request_)
    , dummy_query_string(dag_request->ShortDebugString())
    , dummy_ast(makeDummyQuery())
    , tidb_host(compute_node_host_)
    , collect_execution_summaries(
          dag_request->has_collect_execution_summaries() && dag_request->collect_execution_summaries())
    , kind(DAGRequestKind::Cop)
    , is_root_mpp_task(false)
    , is_disaggregated_task(true)
    , tables_regions_info(std::move(tables_regions_info_))
    , log(std::move(log_))
    , operator_spill_contexts(std::make_shared<TaskOperatorSpillContexts>())
    , flags(dag_request->flags())
    , sql_mode(dag_request->sql_mode())
    , disaggregated_id(std::make_unique<DM::DisaggTaskId>(task_meta_))
    , max_recorded_error_count(getMaxErrorCount(*dag_request))
    , warnings(max_recorded_error_count)
    , warning_count(0)
    , keyspace_id(RequestUtils::deriveKeyspaceID(task_meta_))
    , connection_id(task_meta_.connection_id())
    , connection_alias(task_meta_.connection_alias())
{
    if (dag_request->has_div_precision_increment())
        div_precision_increment = dag_request->div_precision_increment();
    initOutputInfo();
}

// for test
DAGContext::DAGContext(UInt64 max_error_count_)
    : dag_request(nullptr)
    , dummy_ast(makeDummyQuery())
    , collect_execution_summaries(false)
    , kind(DAGRequestKind::Cop)
    , is_root_mpp_task(false)
    , operator_spill_contexts(std::make_shared<TaskOperatorSpillContexts>())
    , flags(0)
    , sql_mode(0)
    , max_recorded_error_count(max_error_count_)
    , warnings(max_recorded_error_count)
    , warning_count(0)
    , connection_id(0)
{}

// for tests need to run query tasks.
DAGContext::DAGContext(tipb::DAGRequest & dag_request_, String log_identifier, size_t concurrency)
    : dag_request(&dag_request_)
    , dummy_query_string(dag_request->ShortDebugString())
    , dummy_ast(makeDummyQuery())
    , initialize_concurrency(concurrency)
    , collect_execution_summaries(
          dag_request->has_collect_execution_summaries() && dag_request->collect_execution_summaries())
    , kind(DAGRequestKind::Cop)
    , is_root_mpp_task(false)
    , log(Logger::get(log_identifier))
    , operator_spill_contexts(std::make_shared<TaskOperatorSpillContexts>())
    , flags(dag_request->flags())
    , sql_mode(dag_request->sql_mode())
    , max_recorded_error_count(getMaxErrorCount(*dag_request))
    , warnings(max_recorded_error_count)
    , warning_count(0)
    , connection_id(0)
{
    if (dag_request->has_div_precision_increment())
        div_precision_increment = dag_request->div_precision_increment();
    query_operator_spill_contexts
        = std::make_shared<QueryOperatorSpillContexts>(MPPQueryId(0, 0, 0, 0, "", 0, ""), 100);
    initOutputInfo();
}

DAGContext::~DAGContext()
{
    operator_spill_contexts->finish();
}

void DAGContext::initOutputInfo()
{
    output_field_types = collectOutputFieldTypes(*dag_request);
    output_offsets.clear();
    result_field_types.clear();
    for (UInt32 i : dag_request->output_offsets())
    {
        output_offsets.push_back(i);
        if (unlikely(i >= output_field_types.size()))
            throw TiFlashException(
                fmt::format(
                    "{}: Invalid output offset(schema has {} columns, access index {}",
                    __PRETTY_FUNCTION__,
                    output_field_types.size(),
                    i),
                Errors::Coprocessor::BadRequest);
        result_field_types.push_back(output_field_types[i]);
    }
    encode_type = analyzeDAGEncodeType();
    keep_session_timezone_info
        = encode_type == tipb::EncodeType::TypeChunk || encode_type == tipb::EncodeType::TypeCHBlock;
}

tipb::EncodeType DAGContext::analyzeDAGEncodeType() const
{
    const tipb::EncodeType request_encode_type = dag_request->encode_type();
    if (isMPPTask() && !isRootMPPTask())
    {
        /// always use CHBlock encode type for data exchange between TiFlash nodes
        return tipb::EncodeType::TypeCHBlock;
    }
    if (dag_request->has_force_encode_type() && dag_request->force_encode_type())
    {
        assert(request_encode_type == tipb::EncodeType::TypeCHBlock);
        return request_encode_type;
    }
    if (isUnsupportedEncodeType(result_field_types, request_encode_type))
        return tipb::EncodeType::TypeDefault;
    if (request_encode_type == tipb::EncodeType::TypeChunk && dag_request->has_chunk_memory_layout()
        && dag_request->chunk_memory_layout().has_endian()
        && dag_request->chunk_memory_layout().endian() == tipb::Endian::BigEndian)
        // todo support BigEndian encode for chunk encode type
        return tipb::EncodeType::TypeDefault;
    return request_encode_type;
}

bool DAGContext::allowZeroInDate() const
{
    return flags & TiDBSQLFlags::IGNORE_ZERO_IN_DATE;
}

bool DAGContext::allowInvalidDate() const
{
    return sql_mode & TiDBSQLMode::ALLOW_INVALID_DATES;
}

void DAGContext::addSubquery(const String & subquery_id, SubqueryForSet && subquery)
{
    SubqueriesForSets subqueries_for_sets;
    subqueries_for_sets[subquery_id] = std::move(subquery);
    subqueries.push_back(std::move(subqueries_for_sets));
}

std::unordered_map<String, BlockInputStreams> & DAGContext::getProfileStreamsMap()
{
    return profile_streams_map;
}

std::unordered_map<String, OperatorProfileInfos> & DAGContext::getOperatorProfileInfosMap()
{
    return operator_profile_infos_map;
}

void DAGContext::addOperatorProfileInfos(
    const String & executor_id,
    OperatorProfileInfos && profile_infos,
    bool is_append)
{
    if (profile_infos.empty())
        return;
    std::lock_guard lock(operator_profile_infos_map_mu);
    if (is_append)
    {
        auto & elem = operator_profile_infos_map[executor_id];
        elem.insert(elem.end(), profile_infos.begin(), profile_infos.end());
    }
    else
    {
        /// The profiles of some operators has been recorded.
        /// For example, `DAGStorageInterpreter` records the profiles of PhysicalTableScan.
        if (operator_profile_infos_map.find(executor_id) == operator_profile_infos_map.end())
            operator_profile_infos_map[executor_id] = std::move(profile_infos);
    }
}

void DAGContext::addInboundIOProfileInfos(
    const String & executor_id,
    IOProfileInfos && io_profile_infos,
    bool is_append)
{
    if (io_profile_infos.empty())
        return;
    std::lock_guard lock(operator_profile_infos_map_mu);
    if (is_append)
    {
        auto & elem = inbound_io_profile_infos_map[executor_id];
        elem.insert(elem.end(), io_profile_infos.begin(), io_profile_infos.end());
    }
    else
    {
        if (inbound_io_profile_infos_map.find(executor_id) == inbound_io_profile_infos_map.end())
            inbound_io_profile_infos_map[executor_id] = std::move(io_profile_infos);
    }
}

std::unordered_map<String, IOProfileInfos> & DAGContext::getInboundIOProfileInfosMap()
{
    return inbound_io_profile_infos_map;
}

void DAGContext::updateFinalConcurrency(size_t cur_streams_size, size_t streams_upper_limit)
{
    final_concurrency = std::min(std::max(final_concurrency, cur_streams_size), streams_upper_limit);
}

void DAGContext::initExecutorIdToJoinIdMap()
{
    // only mpp task has join executor
    // for mpp, all executor has executor id.
    if (!isMPPTask())
        return;

    executor_id_to_join_id_map.clear();
    dag_request.traverseReverse([&](const tipb::Executor & executor) {
        std::vector<String> all_join_id;
        // for mpp, dag_request.has_root_executor() == true, can call `getChildren` directly.
        getChildren(executor).forEach([&](const tipb::Executor & child) {
            assert(child.has_executor_id());
            auto child_it = executor_id_to_join_id_map.find(child.executor_id());
            if (child_it != executor_id_to_join_id_map.end())
                all_join_id.insert(all_join_id.end(), child_it->second.begin(), child_it->second.end());
        });
        assert(executor.has_executor_id());
        if (executor.tp() == tipb::ExecType::TypeJoin)
            all_join_id.push_back(executor.executor_id());
        if (!all_join_id.empty())
            executor_id_to_join_id_map[executor.executor_id()] = all_join_id;
    });
}

std::unordered_map<String, std::vector<String>> & DAGContext::getExecutorIdToJoinIdMap()
{
    return executor_id_to_join_id_map;
}

std::unordered_map<String, JoinExecuteInfo> & DAGContext::getJoinExecuteInfoMap()
{
    return join_execute_info_map;
}

std::unordered_map<String, BlockInputStreams> & DAGContext::getInBoundIOInputStreamsMap()
{
    return inbound_io_input_streams_map;
}

void DAGContext::handleTruncateError(const String & msg)
{
    if (!(flags & TiDBSQLFlags::IGNORE_TRUNCATE || flags & TiDBSQLFlags::TRUNCATE_AS_WARNING))
    {
        throw TiFlashException("Truncate error " + msg, Errors::Types::Truncated);
    }
    appendWarning(msg);
}

void DAGContext::handleOverflowError(const String & msg, const TiFlashError & error)
{
    if (!(flags & TiDBSQLFlags::OVERFLOW_AS_WARNING))
    {
        throw TiFlashException("Overflow error: " + msg, error);
    }
    appendWarning("Overflow error: " + msg);
}

void DAGContext::handleDivisionByZero()
{
    if (flags & TiDBSQLFlags::IN_INSERT_STMT || flags & TiDBSQLFlags::IN_UPDATE_OR_DELETE_STMT)
    {
        if (!(sql_mode & TiDBSQLMode::ERROR_FOR_DIVISION_BY_ZERO))
            return;
        if (strictSqlMode(sql_mode) && !(flags & TiDBSQLFlags::DIVIDED_BY_ZERO_AS_WARNING))
        {
            throw TiFlashException("Division by 0", Errors::Expression::DivisionByZero);
        }
    }
    appendWarning("Division by 0");
}

void DAGContext::handleInvalidTime(const String & msg, const TiFlashError & error)
{
    if (!(error.is(Errors::Types::WrongValue) || error.is(Errors::Types::Truncated)))
    {
        throw TiFlashException(msg, error);
    }
    handleTruncateError(msg);
    if (strictSqlMode(sql_mode)
        && (flags & TiDBSQLFlags::IN_INSERT_STMT || flags & TiDBSQLFlags::IN_UPDATE_OR_DELETE_STMT))
    {
        throw TiFlashException(msg, error);
    }
}

void DAGContext::appendWarning(const String & msg, int32_t code)
{
    tipb::Error warning;
    warning.set_code(code);
    warning.set_msg(msg);
    appendWarning(warning);
}

bool DAGContext::shouldClipToZero() const
{
    return flags & TiDBSQLFlags::IN_INSERT_STMT || flags & TiDBSQLFlags::IN_LOAD_DATA_STMT;
}

std::pair<bool, double> DAGContext::getTableScanThroughput()
{
    if (table_scan_executor_id.empty())
        return std::make_pair(false, 0.0);

    // collect table scan metrics
    UInt64 time_processed_ns = 0;
    UInt64 num_produced_bytes = 0;
    switch (getExecutionMode())
    {
    case ExecutionMode::None:
        break;
    case ExecutionMode::Stream:
        transformProfileForStream(*this, table_scan_executor_id, [&](const IProfilingBlockInputStream & p_stream) {
            time_processed_ns = std::max(time_processed_ns, p_stream.getProfileInfo().execution_time);
            num_produced_bytes += p_stream.getProfileInfo().bytes;
        });
        break;
    case ExecutionMode::Pipeline:
        transformProfileForPipeline(*this, table_scan_executor_id, [&](const OperatorProfileInfo & profile_info) {
            time_processed_ns = std::max(time_processed_ns, profile_info.execution_time);
            num_produced_bytes += profile_info.bytes;
        });
        break;
    }
    // convert to bytes per second
    return std::make_pair(true, num_produced_bytes / (static_cast<double>(time_processed_ns) / 1000000000ULL));
}

ExchangeReceiverPtr DAGContext::getMPPExchangeReceiver(const String & executor_id) const
{
    if (!isMPPTask())
        throw TiFlashException("mpp_exchange_receiver_map is used in mpp only", Errors::Coprocessor::Internal);
    RUNTIME_ASSERT(mpp_receiver_set != nullptr, log, "MPPTask without receiver set");
    return mpp_receiver_set->getExchangeReceiver(executor_id);
}

void DAGContext::addCoprocessorReader(const CoprocessorReaderPtr & coprocessor_reader)
{
    if (!isMPPTask())
        return;
    coprocessor_readers.push_back(coprocessor_reader);
}

std::vector<CoprocessorReaderPtr> & DAGContext::getCoprocessorReaders()
{
    return coprocessor_readers;
}

bool DAGContext::containsRegionsInfoForTable(Int64 table_id) const
{
    return tables_regions_info.containsRegionsInfoForTable(table_id);
}

const SingleTableRegions & DAGContext::getTableRegionsInfoByTableID(Int64 table_id) const
{
    return tables_regions_info.getTableRegionInfoByTableID(table_id);
}

UInt64 DAGContext::getReadBytes() const
{
    UInt64 read_bytes = 0;
    for (const auto & [id, sc] : scan_context_map)
    {
        (void)id; // Disable unused variable warnning.
        read_bytes += sc->user_read_bytes;
    }
    return read_bytes;
}

} // namespace DB

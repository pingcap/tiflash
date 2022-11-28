// Copyright 2022 PingCAP, Ltd.
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
#include <Flash/Coprocessor/collectOutputFieldTypes.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TRUNCATE_ERROR;
extern const int OVERFLOW_ERROR;
extern const int DIVIDED_BY_ZERO;
extern const int INVALID_TIME;
} // namespace ErrorCodes

bool strictSqlMode(UInt64 sql_mode)
{
    return sql_mode & TiDBSQLMode::STRICT_ALL_TABLES || sql_mode & TiDBSQLMode::STRICT_TRANS_TABLES;
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
                fmt::format("{}: Invalid output offset(schema has {} columns, access index {}", __PRETTY_FUNCTION__, output_field_types.size(), i),
                Errors::Coprocessor::BadRequest);
        result_field_types.push_back(output_field_types[i]);
    }
    encode_type = analyzeDAGEncodeType(*this);
    keep_session_timezone_info = encode_type == tipb::EncodeType::TypeChunk || encode_type == tipb::EncodeType::TypeCHBlock;
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
    traverseExecutorsReverse(dag_request, [&](const tipb::Executor & executor) {
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
    if (strictSqlMode(sql_mode) && (flags & TiDBSQLFlags::IN_INSERT_STMT || flags & TiDBSQLFlags::IN_UPDATE_OR_DELETE_STMT))
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
    for (auto & p : getProfileStreamsMap())
    {
        if (p.first == table_scan_executor_id)
        {
            for (auto & stream_ptr : p.second)
            {
                if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream_ptr.get()))
                {
                    time_processed_ns = std::max(time_processed_ns, p_stream->getProfileInfo().execution_time);
                    num_produced_bytes += p_stream->getProfileInfo().bytes;
                }
            }
            break;
        }
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
} // namespace DB

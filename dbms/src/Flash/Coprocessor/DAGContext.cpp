#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
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

bool DAGContext::allowZeroInDate() const
{
    return flags & TiDBSQLFlags::IGNORE_ZERO_IN_DATE;
}

bool DAGContext::allowInvalidDate() const
{
    return sql_mode & TiDBSQLMode::ALLOW_INVALID_DATES;
}

std::map<String, ProfileStreamsInfo> & DAGContext::getProfileStreamsMap()
{
    return profile_streams_map;
}

std::unordered_map<String, BlockInputStreams> & DAGContext::getProfileStreamsMapForJoinBuildSide()
{
    return profile_streams_map_for_join_build_side;
}

std::unordered_map<UInt32, std::vector<String>> & DAGContext::getQBIdToJoinAliasMap()
{
    return qb_id_to_join_alias_map;
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
            for (auto & stream_ptr : p.second.input_streams)
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

void DAGContext::attachBlockIO(const BlockIO & io_)
{
    io = io_;
}
void DAGContext::initExchangeReceiverIfMPP(Context & context, size_t max_streams)
{
    if (isMPPTask())
    {
        if (mpp_exchange_receiver_map_inited)
            throw TiFlashException("Repeatedly initialize mpp_exchange_receiver_map", Errors::Coprocessor::Internal);
        traverseExecutors(dag_request, [&](const tipb::Executor & executor) {
            if (executor.tp() == tipb::ExecType::TypeExchangeReceiver)
            {
                assert(executor.has_executor_id());
                const auto & executor_id = executor.executor_id();
                mpp_exchange_receiver_map[executor_id] = std::make_shared<ExchangeReceiver>(
                    std::make_shared<GRPCReceiverContext>(
                        executor.exchange_receiver(),
                        getMPPTaskMeta(),
                        context.getTMTContext().getKVCluster(),
                        context.getTMTContext().getMPPTaskManager(),
                        context.getSettingsRef().enable_local_tunnel),
                    executor.exchange_receiver().encoded_task_meta_size(),
                    max_streams,
                    log);
            }
            return true;
        });
        mpp_exchange_receiver_map_inited = true;
    }
}

const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & DAGContext::getMPPExchangeReceiverMap() const
{
    if (!isMPPTask())
        throw TiFlashException("mpp_exchange_receiver_map is used in mpp only", Errors::Coprocessor::Internal);
    if (!mpp_exchange_receiver_map_inited)
        throw TiFlashException("mpp_exchange_receiver_map has not been initialized", Errors::Coprocessor::Internal);
    return mpp_exchange_receiver_map;
}

} // namespace DB

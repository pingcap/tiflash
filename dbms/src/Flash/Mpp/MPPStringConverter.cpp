#include <Common/TiFlashException.h>
#include <Flash/Mpp/MPPStringConverter.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace
{
struct CurrentLevelCounter
{
    size_t & current_level;
    explicit CurrentLevelCounter(size_t & level)
        : current_level(level)
    {
        ++current_level;
    }

    ~CurrentLevelCounter()
    {
        --current_level;
    }
};
} // namespace

NamesAndTypes MPPStringConverter::buildTSString(const String & executor_id, const tipb::TableScan & ts, FmtBuffer & buf)
{
    buf.append(genPrefixString());
    return executor_string_converter.buildTSString(executor_id, ts, buf);
}

NamesAndTypes MPPStringConverter::buildSelString(const String & executor_id, const tipb::Selection & sel, FmtBuffer & buf)
{
    return buildUnaryExecutor(
        sel,
        buf,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) -> auto { return executor_string_converter.buildSelString(executor_id, sel, input_column, fb); });
}

NamesAndTypes MPPStringConverter::buildLimitString(const String & executor_id, const tipb::Limit & limit, FmtBuffer & buf)
{
    return buildUnaryExecutor(
        limit,
        buf,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) -> auto { return executor_string_converter.buildLimitString(executor_id, limit, input_column, fb); });
}

NamesAndTypes MPPStringConverter::buildProjString(const String & executor_id, const tipb::Projection & proj, FmtBuffer & buf)
{
    return buildUnaryExecutor(
        proj,
        buf,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) -> auto { return executor_string_converter.buildProjString(executor_id, proj, input_column, fb); });
}

NamesAndTypes MPPStringConverter::buildAggString(const String & executor_id, const tipb::Aggregation & agg, FmtBuffer & buf)
{
    return buildUnaryExecutor(
        agg,
        buf,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) -> auto { return executor_string_converter.buildAggString(executor_id, agg, input_column, fb); });
}

NamesAndTypes MPPStringConverter::buildTopNString(const String & executor_id, const tipb::TopN & top_n, FmtBuffer & buf)
{
    return buildUnaryExecutor(
        top_n,
        buf,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) -> auto { return executor_string_converter.buildTopNString(executor_id, top_n, input_column, fb); });
}

NamesAndTypes MPPStringConverter::buildJoinString(const String & executor_id, const tipb::Join & join, FmtBuffer & buf)
{
    if (join.children_size() != 2)
        throw TiFlashException("Join executor children size not equal to 2", Errors::Coprocessor::BadRequest);

    auto left_input_column = buildString(join.children(0), buf);
    String left_child_str = buf.toString();
    buf.clear();
    auto right_input_column = buildString(join.children(1), buf);
    String right_child_str = buf.toString();
    buf.clear();

    buf.append(genPrefixString());
    auto output_column = executor_string_converter.buildJoinString(executor_id, join, left_input_column, right_input_column, buf);
    buf.append("\n").append(left_child_str).append("\n").append(right_child_str);

    return left_input_column;
}

std::vector<NameAndTypePair> MPPStringConverter::buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, FmtBuffer & buf)
{
    return buildUnaryExecutor(
        exchange_sender,
        buf,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) -> auto { return executor_string_converter.buildExchangeSenderString(executor_id, exchange_sender, input_column, fb); });
}

NamesAndTypes MPPStringConverter::buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, FmtBuffer & buf)
{
    buf.append(genPrefixString());
    return executor_string_converter.buildExchangeReceiverString(executor_id, exchange_receiver, buf);
}

NamesAndTypes MPPStringConverter::buildString(const tipb::Executor & executor, FmtBuffer & buf)
{
    if (!executor.has_executor_id())
        throw TiFlashException("Tree struct based executor must have executor id", Errors::Coprocessor::BadRequest);

    auto counter = CurrentLevelCounter(current_level);
    switch (executor.tp())
    {
    case tipb::ExecType::TypeTableScan:
        return buildTSString(executor.executor_id(), executor.tbl_scan(), buf);
    case tipb::ExecType::TypeJoin:
        return buildJoinString(executor.executor_id(), executor.join(), buf);
    case tipb::ExecType::TypeIndexScan:
        // index scan not supported
        throw TiFlashException("IndexScan is not supported", Errors::Coprocessor::Unimplemented);
    case tipb::ExecType::TypeSelection:
        return buildSelString(executor.executor_id(), executor.selection(), buf);
    case tipb::ExecType::TypeAggregation:
    // stream agg is not supported, treated as normal agg
    case tipb::ExecType::TypeStreamAgg:
        return buildAggString(executor.executor_id(), executor.aggregation(), buf);
    case tipb::ExecType::TypeTopN:
        return buildTopNString(executor.executor_id(), executor.topn(), buf);
    case tipb::ExecType::TypeLimit:
        return buildLimitString(executor.executor_id(), executor.limit(), buf);
    case tipb::ExecType::TypeProjection:
        return buildProjString(executor.executor_id(), executor.projection(), buf);
    case tipb::ExecType::TypeExchangeSender:
        return buildExchangeSenderString(executor.executor_id(), executor.exchange_sender(), buf);
    case tipb::ExecType::TypeExchangeReceiver:
        return buildExchangeReceiverString(executor.executor_id(), executor.exchange_receiver(), buf);
    case tipb::ExecType::TypeKill:
        throw TiFlashException("Kill executor is not supported", Errors::Coprocessor::Unimplemented);
    default:
        throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
    }
}

MPPStringConverter::MPPStringConverter(Context & context_, const tipb::DAGRequest & dag_request_)
    : executor_string_converter(context_)
    , dag_request(dag_request_)
{}

String MPPStringConverter::buildMPPString()
{
    if (!dag_request.has_root_executor())
        throw TiFlashException("dag_request is illegal for mpp query", Errors::Coprocessor::BadRequest);

    const tipb::Executor & executor = dag_request.root_executor();
    FmtBuffer mpp_buf;
    buildString(executor, mpp_buf);
    return mpp_buf.toString();
}

} // namespace DB
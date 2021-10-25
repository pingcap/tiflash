#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Flash/ExecutorToStringUtils.h>
#include <Flash/Mpp/MPPStringConverter.h>

#include <functional>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace
{
struct LevelCounter
{
    size_t & level;
    explicit LevelCounter(size_t & level_)
        : level(level_)
    {
        ++level;
    }

    ~LevelCounter()
    {
        --level;
    }
};

String genPrefixString(size_t level)
{
    return String(2 * level, ' ');
}

NamesAndTypes buildString(const tipb::Executor & executor, FmtBuffer & buf, size_t & level, Context & context);

template <typename Unary, typename FF>
NamesAndTypes buildUnaryExecutor(const Unary & unary, FmtBuffer & buf, size_t & level, Context & context, FF build_self_func)
{
    auto input_column = buildString(unary.child(), buf, level, context);
    String child_str = buf.toString();
    buf.clear();

    buf.append(genPrefixString(level));
    auto output_column = build_self_func(buf, input_column);
    buf.append("\n").append(child_str);
    return output_column;
}

NamesAndTypes buildTSString(const String & executor_id, const tipb::TableScan & ts, FmtBuffer & buf, size_t & level, Context & context)
{
    buf.append(genPrefixString(level));
    return buildTSString(executor_id, ts, context, buf);
}

NamesAndTypes buildSelString(const String & executor_id, const tipb::Selection & sel, FmtBuffer & buf, size_t & level, Context & context)
{
    return buildUnaryExecutor(
        sel,
        buf,
        level,
        context,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) { return buildSelString(executor_id, sel, input_column, fb); });
}

NamesAndTypes buildLimitString(const String & executor_id, const tipb::Limit & limit, FmtBuffer & buf, size_t & level, Context & context)
{
    return buildUnaryExecutor(
        limit,
        buf,
        level,
        context,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) { return buildLimitString(executor_id, limit, input_column, fb); });
}

NamesAndTypes buildProjString(const String & executor_id, const tipb::Projection & proj, FmtBuffer & buf, size_t & level, Context & context)
{
    return buildUnaryExecutor(
        proj,
        buf,
        level,
        context,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) { return buildProjString(executor_id, proj, input_column, fb); });
}

NamesAndTypes buildAggString(const String & executor_id, const tipb::Aggregation & agg, FmtBuffer & buf, size_t & level, Context & context)
{
    return buildUnaryExecutor(
        agg,
        buf,
        level,
        context,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) { return buildAggString(executor_id, agg, input_column, fb); });
}

NamesAndTypes buildTopNString(const String & executor_id, const tipb::TopN & top_n, FmtBuffer & buf, size_t & level, Context & context)
{
    return buildUnaryExecutor(
        top_n,
        buf,
        level,
        context,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) { return buildTopNString(executor_id, top_n, input_column, fb); });
}

NamesAndTypes buildJoinString(const String & executor_id, const tipb::Join & join, FmtBuffer & buf, size_t & level, Context & context)
{
    if (join.children_size() != 2)
        throw TiFlashException("Join executor children size not equal to 2", Errors::Coprocessor::BadRequest);

    auto left_input_column = buildString(join.children(0), buf, level, context);
    String left_child_str = buf.toString();
    buf.clear();
    auto right_input_column = buildString(join.children(1), buf, level, context);
    String right_child_str = buf.toString();
    buf.clear();

    buf.append(genPrefixString(level));
    auto output_column = buildJoinString(executor_id, join, left_input_column, right_input_column, buf);
    buf.append("\n").append(left_child_str).append("\n").append(right_child_str);

    return left_input_column;
}

NamesAndTypes buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, FmtBuffer & buf, size_t & level, Context & context)
{
    return buildUnaryExecutor(
        exchange_sender,
        buf,
        level,
        context,
        [&](FmtBuffer & fb, NamesAndTypes & input_column) { return buildExchangeSenderString(executor_id, exchange_sender, input_column, fb); });
}

NamesAndTypes buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, FmtBuffer & buf, size_t & level)
{
    buf.append(genPrefixString(level));
    return buildExchangeReceiverString(executor_id, exchange_receiver, buf);
}

NamesAndTypes buildString(const tipb::Executor & executor, FmtBuffer & buf, size_t & level, Context & context)
{
    if (!executor.has_executor_id())
        throw TiFlashException("Tree struct based executor must have executor id", Errors::Coprocessor::BadRequest);

    auto counter = LevelCounter(level);
    switch (executor.tp())
    {
    case tipb::ExecType::TypeTableScan:
        return buildTSString(executor.executor_id(), executor.tbl_scan(), buf, level, context);
    case tipb::ExecType::TypeJoin:
        return buildJoinString(executor.executor_id(), executor.join(), buf, level, context);
    case tipb::ExecType::TypeIndexScan:
        // index scan not supported
        throw TiFlashException("IndexScan is not supported", Errors::Coprocessor::Unimplemented);
    case tipb::ExecType::TypeSelection:
        return buildSelString(executor.executor_id(), executor.selection(), buf, level, context);
    case tipb::ExecType::TypeAggregation:
    // stream agg is not supported, treated as normal agg
    case tipb::ExecType::TypeStreamAgg:
        return buildAggString(executor.executor_id(), executor.aggregation(), buf, level, context);
    case tipb::ExecType::TypeTopN:
        return buildTopNString(executor.executor_id(), executor.topn(), buf, level, context);
    case tipb::ExecType::TypeLimit:
        return buildLimitString(executor.executor_id(), executor.limit(), buf, level, context);
    case tipb::ExecType::TypeProjection:
        return buildProjString(executor.executor_id(), executor.projection(), buf, level, context);
    case tipb::ExecType::TypeExchangeSender:
        return buildExchangeSenderString(executor.executor_id(), executor.exchange_sender(), buf, level, context);
    case tipb::ExecType::TypeExchangeReceiver:
        return buildExchangeReceiverString(executor.executor_id(), executor.exchange_receiver(), buf, level);
    case tipb::ExecType::TypeKill:
        throw TiFlashException("Kill executor is not supported", Errors::Coprocessor::Unimplemented);
    default:
        throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
    }
}
} // namespace

String MPPStringConverter::buildMPPString()
{
    if (!dag_request.has_root_executor())
        throw TiFlashException("dag_request is illegal for mpp query", Errors::Coprocessor::BadRequest);

    const tipb::Executor & executor = dag_request.root_executor();
    FmtBuffer mpp_buf;
    size_t level = 0;
    buildString(executor, mpp_buf, level, context);
    return mpp_buf.toString();
}

MPPStringConverter::MPPStringConverter(Context & context_, const tipb::DAGRequest & dag_request_)
    : context(context_)
    , dag_request(dag_request_)
{}

} // namespace DB
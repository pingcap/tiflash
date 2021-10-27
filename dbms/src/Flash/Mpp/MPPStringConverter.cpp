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

void buildString(const tipb::Executor & executor, size_t & level, BuildContext & build_context);

template <typename Unary, typename FF>
void buildUnaryExecutor(const Unary & unary, size_t & level, BuildContext & build_context, FF && build_self_func)
{
    auto & buf = build_context.buf;
    buildString(unary.child(), level, build_context);
    String child_str = buf.toString();
    buf.clear();

    buf.append(genPrefixString(level));
    build_self_func();
    buf.append("\n").append(child_str);
}

void buildTSString(const String & executor_id, const tipb::TableScan & ts, size_t & level, BuildContext & build_context)
{
    build_context.buf.append(genPrefixString(level));
    buildTSString(executor_id, ts, build_context);
}

void buildSelString(const String & executor_id, const tipb::Selection & sel, size_t & level, BuildContext & build_context)
{
    buildUnaryExecutor(
        sel,
        level,
        build_context,
        [&]() { buildSelString(executor_id, sel, build_context); });
}

void buildLimitString(const String & executor_id, const tipb::Limit & limit, size_t & level, BuildContext & build_context)
{
    buildUnaryExecutor(
        limit,
        level,
        build_context,
        [&]() { buildLimitString(executor_id, limit, build_context); });
}

void buildProjString(const String & executor_id, const tipb::Projection & proj, size_t & level, BuildContext & build_context)
{
    buildUnaryExecutor(
        proj,
        level,
        build_context,
        [&]() { buildProjString(executor_id, proj, build_context); });
}

void buildAggString(const String & executor_id, const tipb::Aggregation & agg, size_t & level, BuildContext & build_context)
{
    buildUnaryExecutor(
        agg,
        level,
        build_context,
        [&]() { buildAggString(executor_id, agg, build_context); });
}

void buildTopNString(const String & executor_id, const tipb::TopN & top_n, size_t & level, BuildContext & build_context)
{
    buildUnaryExecutor(
        top_n,
        level,
        build_context,
        [&]() { buildTopNString(executor_id, top_n, build_context); });
}

void buildJoinString(const String & executor_id, const tipb::Join & join, size_t & level, BuildContext & build_context)
{
    if (join.children_size() != 2)
        throw TiFlashException("Join executor children size not equal to 2", Errors::Coprocessor::BadRequest);

    auto & buf = build_context.buf;
    buildString(join.children(0), level, build_context);
    String left_child_str = buf.toString();
    buf.clear();
    buildString(join.children(1), level, build_context);
    String right_child_str = buf.toString();
    buf.clear();

    buf.append(genPrefixString(level));
    buildJoinString(executor_id, join, build_context);
    buf.append("\n").append(left_child_str).append("\n").append(right_child_str);
}

void buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, size_t & level, BuildContext & build_context)
{
    buildUnaryExecutor(
        exchange_sender,
        level,
        build_context,
        [&]() { buildExchangeSenderString(executor_id, exchange_sender, build_context); });
}

void buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, size_t & level, BuildContext & build_context)
{
    build_context.buf.append(genPrefixString(level));
    buildExchangeReceiverString(executor_id, exchange_receiver, build_context);
}

void buildString(const tipb::Executor & executor, size_t & level, BuildContext & build_context)
{
    if (!executor.has_executor_id())
        throw TiFlashException("Tree struct based executor must have executor id", Errors::Coprocessor::BadRequest);

    auto counter = LevelCounter(level);
    switch (executor.tp())
    {
    case tipb::ExecType::TypeTableScan:
        buildTSString(executor.executor_id(), executor.tbl_scan(), level, build_context);
        break;
    case tipb::ExecType::TypeJoin:
        buildJoinString(executor.executor_id(), executor.join(), level, build_context);
        break;
    case tipb::ExecType::TypeIndexScan:
        // index scan not supported
        throw TiFlashException("IndexScan is not supported", Errors::Coprocessor::Unimplemented);
    case tipb::ExecType::TypeSelection:
        buildSelString(executor.executor_id(), executor.selection(), level, build_context);
        break;
    case tipb::ExecType::TypeAggregation:
    // stream agg is not supported, treated as normal agg
    case tipb::ExecType::TypeStreamAgg:
        buildAggString(executor.executor_id(), executor.aggregation(), level, build_context);
        break;
    case tipb::ExecType::TypeTopN:
        buildTopNString(executor.executor_id(), executor.topn(), level, build_context);
        break;
    case tipb::ExecType::TypeLimit:
        buildLimitString(executor.executor_id(), executor.limit(), level, build_context);
        break;
    case tipb::ExecType::TypeProjection:
        buildProjString(executor.executor_id(), executor.projection(), level, build_context);
        break;
    case tipb::ExecType::TypeExchangeSender:
        buildExchangeSenderString(executor.executor_id(), executor.exchange_sender(), level, build_context);
        break;
    case tipb::ExecType::TypeExchangeReceiver:
        buildExchangeReceiverString(executor.executor_id(), executor.exchange_receiver(), level, build_context);
        break;
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
    BuildContext build_context{context, mpp_buf};
    size_t level = 0;
    buildString(executor, level, build_context);
    return mpp_buf.toString();
}

MPPStringConverter::MPPStringConverter(Context & context_, const tipb::DAGRequest & dag_request_)
    : context(context_)
    , dag_request(dag_request_)
{}

} // namespace DB
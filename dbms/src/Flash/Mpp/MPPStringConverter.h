#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Common/FmtUtils.h>
#include <Core/NamesAndTypes.h>
#include <Flash/ExecutorStringConverter.h>

#include <functional>

namespace DB
{
class Context;

class MPPStringConverter
{
public:
    MPPStringConverter(Context & context_, const tipb::DAGRequest & dag_request_);

    ~MPPStringConverter() = default;

    String buildMPPString();

private:
    template <typename Unary, typename FF>
    NamesAndTypes buildUnaryExecutor(const Unary & unary, FmtBuffer & buf, FF build_self_func)
    {
        auto input_column = buildString(unary.child(), buf);
        String child_str = buf.toString();
        buf.clear();

        buf.append(genPrefixString());
        auto output_column = build_self_func(buf, input_column);
        buf.append("\n").append(child_str);
        return output_column;
    }

    NamesAndTypes buildTSString(const String & executor_id, const tipb::TableScan & ts, FmtBuffer & buf);
    NamesAndTypes buildSelString(const String & executor_id, const tipb::Selection & sel, FmtBuffer & buf);
    NamesAndTypes buildLimitString(const String & executor_id, const tipb::Limit & limit, FmtBuffer & buf);
    NamesAndTypes buildProjString(const String & executor_id, const tipb::Projection & proj, FmtBuffer & buf);
    NamesAndTypes buildAggString(const String & executor_id, const tipb::Aggregation & agg, FmtBuffer & buf);
    NamesAndTypes buildTopNString(const String & executor_id, const tipb::TopN & top_n, FmtBuffer & buf);
    NamesAndTypes buildJoinString(const String & executor_id, const tipb::Join & join, FmtBuffer & buf);
    NamesAndTypes buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, FmtBuffer & buf);
    NamesAndTypes buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, FmtBuffer & buf);
    NamesAndTypes buildString(const tipb::Executor & executor, FmtBuffer & buf);

    String genPrefixString() const { return String(2 * current_level, ' '); }

    ExecutorStringConverter executor_string_converter;

    const tipb::DAGRequest & dag_request;

    size_t current_level = 0;
};

} // namespace DB
#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Common/FmtUtils.h>
#include <Core/NamesAndTypes.h>

namespace DB
{
class Context;

class ExecutorStringConverter
{
public:
    explicit ExecutorStringConverter(Context & context_);

    ~ExecutorStringConverter() = default;

    NamesAndTypes buildTSString(const String & executor_id, const tipb::TableScan & ts, FmtBuffer & buf);
    static NamesAndTypes buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, FmtBuffer & buf);

    static NamesAndTypes buildSelString(const String & executor_id, const tipb::Selection & sel, NamesAndTypes & input_column, FmtBuffer & buf);
    static NamesAndTypes buildLimitString(const String & executor_id, const tipb::Limit & limit, NamesAndTypes & input_column, FmtBuffer & buf);
    static NamesAndTypes buildProjString(const String & executor_id, const tipb::Projection & proj, NamesAndTypes & input_column, FmtBuffer & buf);
    static NamesAndTypes buildAggString(const String & executor_id, const tipb::Aggregation & agg, NamesAndTypes & input_column, FmtBuffer & buf);
    static NamesAndTypes buildTopNString(const String & executor_id, const tipb::TopN & top_n, NamesAndTypes & input_column, FmtBuffer & buf);
    static NamesAndTypes buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, NamesAndTypes & input_column, FmtBuffer & buf);

    static NamesAndTypes buildJoinString(const String & executor_id, const tipb::Join & join, NamesAndTypes & left_input_column, NamesAndTypes & right_input_column, FmtBuffer & buf);

private:
    Context & context;
};

} // namespace DB
#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Core/NamesAndTypes.h>

namespace DB
{
class Context;

NamesAndTypes buildTSString(const String & executor_id, const tipb::TableScan & ts, Context & context, FmtBuffer & buf);
NamesAndTypes buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, FmtBuffer & buf);

NamesAndTypes & buildSelString(const String & executor_id, const tipb::Selection & sel, NamesAndTypes & input_column, FmtBuffer & buf);
NamesAndTypes & buildLimitString(const String & executor_id, const tipb::Limit & limit, NamesAndTypes & input_column, FmtBuffer & buf);
NamesAndTypes buildProjString(const String & executor_id, const tipb::Projection & proj, NamesAndTypes & input_column, FmtBuffer & buf);
NamesAndTypes buildAggString(const String & executor_id, const tipb::Aggregation & agg, NamesAndTypes & input_column, FmtBuffer & buf);
NamesAndTypes & buildTopNString(const String & executor_id, const tipb::TopN & top_n, NamesAndTypes & input_column, FmtBuffer & buf);
NamesAndTypes & buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, NamesAndTypes & input_column, FmtBuffer & buf);

NamesAndTypes & buildJoinString(const String & executor_id, const tipb::Join & join, NamesAndTypes & left_input_column, NamesAndTypes & right_input_column, FmtBuffer & buf);

} // namespace DB
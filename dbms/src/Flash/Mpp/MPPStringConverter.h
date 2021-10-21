#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>
#include <IO/WriteBufferFromString.h>

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
    NamesAndTypes buildTSString(const String & executor_id, const tipb::TableScan & ts, WriteBufferFromOwnString & buf);
    NamesAndTypes buildSelString(const String & executor_id, const tipb::Selection & sel, WriteBufferFromOwnString & buf);
    NamesAndTypes buildLimitString(const String & executor_id, const tipb::Limit & limit, WriteBufferFromOwnString & buf);
    NamesAndTypes buildProjString(const String & executor_id, const tipb::Projection & proj, WriteBufferFromOwnString & buf);
    NamesAndTypes buildAggString(const String & executor_id, const tipb::Aggregation & agg, WriteBufferFromOwnString & buf);
    NamesAndTypes buildTopNString(const String & executor_id, const tipb::TopN & topN, WriteBufferFromOwnString & buf);
    NamesAndTypes buildJoinString(const String & executor_id, const tipb::Join & join, WriteBufferFromOwnString & buf);
    NamesAndTypes buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, WriteBufferFromOwnString & buf);
    NamesAndTypes buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, WriteBufferFromOwnString & buf);
    NamesAndTypes buildString(const tipb::Executor & executor, WriteBufferFromOwnString & buf);

    String genPrefixString() const { return String(2 * current_level, ' '); }

    Context & context;
    const tipb::DAGRequest & dag_request;

    size_t current_level = 0;
};

} // namespace DB
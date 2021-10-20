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
    NamesAndTypes buildTSString(const String & executor_id, const tipb::TableScan & ts, WriteBufferFromOwnString & ss);
    NamesAndTypes buildSelString(const String & executor_id, const tipb::Selection & sel, WriteBufferFromOwnString & ss);
    NamesAndTypes buildLimitString(const String & executor_id, const tipb::Limit & limit, WriteBufferFromOwnString & ss);
    NamesAndTypes buildProjString(const String & executor_id, const tipb::Projection & proj, WriteBufferFromOwnString & ss);
    NamesAndTypes buildAggString(const String & executor_id, const tipb::Aggregation & agg, WriteBufferFromOwnString & ss);
    NamesAndTypes buildTopNString(const String & executor_id, const tipb::TopN & topN, WriteBufferFromOwnString & ss);
    NamesAndTypes buildJoinString(const String & executor_id, const tipb::Join & join, WriteBufferFromOwnString & ss);
    NamesAndTypes buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, WriteBufferFromOwnString & ss);
    NamesAndTypes buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, WriteBufferFromOwnString & ss);
    NamesAndTypes buildString(const tipb::Executor & executor, WriteBufferFromOwnString & ss);

    String genPrefixString() const { return String(2 * current_level, ' '); }

    Context & context;
    const tipb::DAGRequest & dag_request;

    size_t current_level = 0;
};

} // namespace DB
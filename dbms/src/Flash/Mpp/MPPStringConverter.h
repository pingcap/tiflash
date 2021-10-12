#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>

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
    NamesAndTypes buildTSString(const tipb::TableScan & ts, std::stringstream & ss);
    NamesAndTypes buildSelString(const tipb::Selection & sel, std::stringstream & ss);
    NamesAndTypes buildLimitString(const tipb::Limit & limit, std::stringstream & ss);
    NamesAndTypes buildProjString(const tipb::Projection & proj, std::stringstream & ss);
    NamesAndTypes buildAggString(const tipb::Aggregation & agg, std::stringstream & ss);
    NamesAndTypes buildTopNString(const tipb::TopN & topN, std::stringstream & ss);
    NamesAndTypes buildJoinString(const tipb::Join & join, std::stringstream & ss);
    NamesAndTypes buildExchangeSenderString(const tipb::ExchangeSender & exchange_sender, std::stringstream & ss);
    NamesAndTypes buildExchangeReceiverString(const tipb::ExchangeReceiver & exchange_receiver, std::stringstream & ss);
    NamesAndTypes buildString(const tipb::Executor & executor, std::stringstream & ss);

    String genPrefixString() { return String(2 * current_level, ' '); }

    Context & context;
    const tipb::DAGRequest & dag_request;

    size_t current_level;
};

} // namespace DB

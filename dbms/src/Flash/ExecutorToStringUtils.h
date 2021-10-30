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

struct BuildContext
{
    Context & context;
    FmtBuffer & buf;

    BuildContext(Context & context_, FmtBuffer & buf_)
        : context(context_)
        , buf(buf_)
    {}

    std::vector<NamesAndTypes> schemas;

    NamesAndTypes & schema()
    {
        return schemas.back();
    }

    NamesAndTypes popBackSchema()
    {
        NamesAndTypes pop_back_schema = schema();
        schemas.pop_back();
        return pop_back_schema;
    }

    NamesAndTypes & newSchema()
    {
        schemas.emplace_back();
        return schema();
    }
};

void buildTSString(const String & executor_id, const tipb::TableScan & ts, BuildContext & build_context);
void buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, BuildContext & build_context);
void buildSelString(const String & executor_id, const tipb::Selection & sel, BuildContext & build_context);
void buildLimitString(const String & executor_id, const tipb::Limit & limit, BuildContext & build_context);
void buildProjString(const String & executor_id, const tipb::Projection & proj, BuildContext & build_context);
void buildAggString(const String & executor_id, const tipb::Aggregation & agg, BuildContext & build_context);
void buildTopNString(const String & executor_id, const tipb::TopN & top_n, BuildContext & build_context);
void buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, BuildContext & build_context);
void buildJoinString(const String & executor_id, const tipb::Join & join, BuildContext & build_context);

} // namespace DB
#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Interpreters/IQuerySource.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

class Context;

/// Query source of a DAG request via gRPC.
/// This is also an IR of a DAG.
class DAGQuerySource : public IQuerySource
{
public:
    static const String TS_NAME;
    static const String SEL_NAME;
    static const String AGG_NAME;
    static const String TOPN_NAME;
    static const String LIMIT_NAME;

    DAGQuerySource(Context & context_, RegionID region_id_, UInt64 region_version_, UInt64 region_conf_version_,
        const tipb::DAGRequest & dag_request_);

    std::tuple<std::string, ASTPtr> parse(size_t max_query_size) override;
    String str(size_t max_query_size) override;
    std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) override;

    void assertValid(Int32 index, const String & name)
    {
        if (index < 0 || index > dag_request.executors_size())
        {
            throw Exception("Access invalid executor: " + name);
        }
    }

    RegionID getRegionID() const { return region_id; }
    UInt64 getRegionVersion() const { return region_version; }
    UInt64 getRegionConfVersion() const { return region_conf_version; }

    bool has_selection() { return sel_index != -1; };
    bool has_aggregation() { return agg_index != -1; };
    bool has_topN() { return order_index != -1; };
    bool has_limit() { return order_index == -1 && limit_index != -1; };

    const tipb::TableScan & get_ts()
    {
        assertValid(ts_index, TS_NAME);
        return dag_request.executors(ts_index).tbl_scan();
    };
    const tipb::Selection & get_sel()
    {
        assertValid(sel_index, SEL_NAME);
        return dag_request.executors(sel_index).selection();
    };
    const tipb::Aggregation & get_agg()
    {
        assertValid(agg_index, AGG_NAME);
        return dag_request.executors(agg_index).aggregation();
    };
    const tipb::TopN & get_topN()
    {
        assertValid(order_index, TOPN_NAME);
        return dag_request.executors(order_index).topn();
    };
    const tipb::Limit & get_limit()
    {
        assertValid(limit_index, LIMIT_NAME);
        return dag_request.executors(limit_index).limit();
    };
    const tipb::DAGRequest & get_dag_request() { return dag_request; };

protected:
    Context & context;

    const RegionID region_id;
    const UInt64 region_version;
    const UInt64 region_conf_version;

    const tipb::DAGRequest & dag_request;

    Int32 ts_index = -1;
    Int32 sel_index = -1;
    Int32 agg_index = -1;
    Int32 order_index = -1;
    Int32 limit_index = -1;
};

} // namespace DB

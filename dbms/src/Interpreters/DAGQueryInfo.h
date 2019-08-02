#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Coprocessor/CoprocessorHandler.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/IQueryInfo.h>
#include <Parsers/IAST.h>


namespace DB
{

/** DAGQueryInfo for query represented by DAG request.
  */
class DAGQueryInfo : public IQueryInfo
{
public:
    static const String TS_NAME;
    static const String SEL_NAME;
    static const String AGG_NAME;
    static const String TOPN_NAME;
    static const String LIMIT_NAME;

    DAGQueryInfo(const tipb::DAGRequest & dag_request, CoprocessorContext & coprocessorContext_);
    bool isInternalQuery() { return false; };
    virtual std::tuple<std::string, ASTPtr> parse(size_t max_query_size);
    virtual String get_query_ignore_error(size_t max_query_size);
    virtual std::unique_ptr<IInterpreter> getInterpreter(Context & context, QueryProcessingStage::Enum stage);
    void assertValid(Int32 index, const String & name)
    {
        if (index < 0 || index > dag_request.executors_size())
        {
            throw Exception("Access invalid executor: " + name);
        }
    }
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

private:
    const tipb::DAGRequest & dag_request;
    CoprocessorContext & coprocessorContext;
    String query;
    ASTPtr ast;
    Int32 ts_index = -1;
    Int32 sel_index = -1;
    Int32 agg_index = -1;
    Int32 order_index = -1;
    Int32 limit_index = -1;
};

} // namespace DB

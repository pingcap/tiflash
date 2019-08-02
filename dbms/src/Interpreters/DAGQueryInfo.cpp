
#include <Interpreters/DAGQueryInfo.h>
#include <Interpreters/InterpreterDAGRequest.h>
#include <Parsers/ASTSelectQuery.h>


namespace DB
{

const String DAGQueryInfo::TS_NAME("tablescan");
const String DAGQueryInfo::SEL_NAME("selection");
const String DAGQueryInfo::AGG_NAME("aggregation");
const String DAGQueryInfo::TOPN_NAME("topN");
const String DAGQueryInfo::LIMIT_NAME("limit");

static void assignOrThrowException(Int32 & index, Int32 value, const String & name)
{
    if (index != -1)
    {
        throw Exception("Duplicated " + name + " in DAG request");
    }
    index = value;
}

DAGQueryInfo::DAGQueryInfo(const tipb::DAGRequest & dag_request_, CoprocessorContext & coprocessorContext_)
    : dag_request(dag_request_), coprocessorContext(coprocessorContext_)
{
    for (int i = 0; i < dag_request.executors_size(); i++)
    {
        switch (dag_request.executors(i).tp())
        {
            case tipb::ExecType::TypeTableScan:
                assignOrThrowException(ts_index, i, TS_NAME);
                break;
            case tipb::ExecType::TypeSelection:
                assignOrThrowException(sel_index, i, SEL_NAME);
                break;
            case tipb::ExecType::TypeStreamAgg:
            case tipb::ExecType::TypeAggregation:
                assignOrThrowException(agg_index, i, AGG_NAME);
                break;
            case tipb::ExecType::TypeTopN:
                assignOrThrowException(order_index, i, TOPN_NAME);
            case tipb::ExecType::TypeLimit:
                assignOrThrowException(limit_index, i, LIMIT_NAME);
                break;
            default:
                throw Exception("Unsupported executor in DAG request: " + dag_request.executors(i).DebugString());
        }
    }
}

std::tuple<std::string, ASTPtr> DAGQueryInfo::parse(size_t)
{
    query = String("cop query");
    ast = std::make_shared<ASTSelectQuery>();
    ((ASTSelectQuery *)ast.get())->is_fake_sel = true;
    return std::make_tuple(query, ast);
}

String DAGQueryInfo::get_query_ignore_error(size_t) { return query; }

std::unique_ptr<IInterpreter> DAGQueryInfo::getInterpreter(Context &, QueryProcessingStage::Enum)
{
    return std::make_unique<InterpreterDAGRequest>(coprocessorContext, *this);
}
} // namespace DB

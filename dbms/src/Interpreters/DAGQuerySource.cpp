#include <Interpreters/DAGQuerySource.h>
#include <Interpreters/InterpreterDAG.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

const String DAGQuerySource::TS_NAME("tablescan");
const String DAGQuerySource::SEL_NAME("selection");
const String DAGQuerySource::AGG_NAME("aggregation");
const String DAGQuerySource::TOPN_NAME("topN");
const String DAGQuerySource::LIMIT_NAME("limit");

static void assignOrThrowException(Int32 & index, Int32 value, const String & name)
{
    if (index != -1)
    {
        throw Exception("Duplicated " + name + " in DAG request");
    }
    index = value;
}

DAGQuerySource::DAGQuerySource(
    Context & context_, RegionID region_id_, UInt64 region_version_, UInt64 region_conf_version_, const tipb::DAGRequest & dag_request_)
    : context(context_),
      region_id(region_id_),
      region_version(region_version_),
      region_conf_version(region_conf_version_),
      dag_request(dag_request_)
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

std::tuple<std::string, ASTPtr> DAGQuerySource::parse(size_t max_query_size)
{
    String tmp = "select 1";
    ParserQuery parser(tmp.data() + tmp.size());
    ASTPtr parent = parseQuery(parser, tmp.data(), tmp.data() + tmp.size(), "", max_query_size);
    auto query = dag_request.DebugString();
    ast = ((ASTSelectWithUnionQuery *)parent.get())->list_of_selects->children.at(0);
    return std::make_tuple(query, ast);
}

String DAGQuerySource::str(size_t) { return dag_request.DebugString(); }

std::unique_ptr<IInterpreter> DAGQuerySource::interpreter(Context &, QueryProcessingStage::Enum)
{
    return std::make_unique<InterpreterDAG>(context, *this);
}

FieldTpAndFlags DAGQuerySource::getOutputFieldTpAndFlags() const
{
    FieldTpAndFlags output;

    const auto & ts = getTS();
    const auto & column_infos = ts.columns();
    for (auto i : dag_request.output_offsets())
    {
        // TODO: Checking bound.
        auto & column_info = column_infos[i];
        output.emplace_back(FieldTpAndFlag{static_cast<TiDB::TP>(column_info.tp()), static_cast<UInt32>(column_info.flag())});
    }

    // TODO: Add aggregation columns.
    // We either write our own code to infer types that follows the convention between TiDB and TiKV, or ask TiDB to push down aggregation field types.

    return output;
}

} // namespace DB

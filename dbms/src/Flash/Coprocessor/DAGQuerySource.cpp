#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

namespace ErrorCodes
{
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes

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

DAGQuerySource::DAGQuerySource(Context & context_, DAGContext & dag_context_, RegionID region_id_, UInt64 region_version_,
    UInt64 region_conf_version_, const std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> & key_ranges_,
    const tipb::DAGRequest & dag_request_)
    : context(context_),
      dag_context(dag_context_),
      region_id(region_id_),
      region_version(region_version_),
      region_conf_version(region_conf_version_),
      key_ranges(key_ranges_),
      dag_request(dag_request_),
      metrics(context.getTiFlashMetrics())
{
    for (int i = 0; i < dag_request.executors_size(); i++)
    {
        switch (dag_request.executors(i).tp())
        {
            case tipb::ExecType::TypeTableScan:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_ts).Increment();
                assignOrThrowException(ts_index, i, TS_NAME);
                break;
            case tipb::ExecType::TypeSelection:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_sel).Increment();
                assignOrThrowException(sel_index, i, SEL_NAME);
                break;
            case tipb::ExecType::TypeStreamAgg:
            case tipb::ExecType::TypeAggregation:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_agg).Increment();
                assignOrThrowException(agg_index, i, AGG_NAME);
                break;
            case tipb::ExecType::TypeTopN:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_topn).Increment();
                assignOrThrowException(order_index, i, TOPN_NAME);
                assignOrThrowException(limit_index, i, TOPN_NAME);
                break;
            case tipb::ExecType::TypeLimit:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_limit).Increment();
                assignOrThrowException(limit_index, i, LIMIT_NAME);
                break;
            default:
                throw Exception(
                    "Unsupported executor in DAG request: " + dag_request.executors(i).DebugString(), ErrorCodes::NOT_IMPLEMENTED);
        }
    }
    analyzeResultFieldTypes();
    analyzeDAGEncodeType();
}

void DAGQuerySource::analyzeDAGEncodeType()
{
    encode_type = dag_request.encode_type();
    if (isUnsupportedEncodeType(getResultFieldTypes(), encode_type))
        encode_type = tipb::EncodeType::TypeDefault;
}

std::tuple<std::string, ASTPtr> DAGQuerySource::parse(size_t max_query_size)
{
    // this is a WAR to avoid NPE when the MergeTreeDataSelectExecutor trying
    // to extract key range of the query.
    // todo find a way to enable key range extraction for dag query
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

bool fillExecutorOutputFieldTypes(const tipb::Executor & executor, std::vector<tipb::FieldType> & output_field_types)
{
    tipb::FieldType field_type;
    switch (executor.tp())
    {
        case tipb::ExecType::TypeTableScan:
            for (auto & ci : executor.tbl_scan().columns())
            {
                field_type.set_tp(ci.tp());
                field_type.set_flag(ci.flag());
                field_type.set_flen(ci.columnlen());
                field_type.set_decimal(ci.decimal());
                output_field_types.push_back(field_type);
            }
            return true;
        case tipb::ExecType::TypeStreamAgg:
        case tipb::ExecType::TypeAggregation:
            for (auto & expr : executor.aggregation().agg_func())
            {
                if (!exprHasValidFieldType(expr))
                {
                    throw Exception("Agg expression without valid field type", ErrorCodes::COP_BAD_DAG_REQUEST);
                }
                output_field_types.push_back(expr.field_type());
            }
            for (auto & expr : executor.aggregation().group_by())
            {
                if (!exprHasValidFieldType(expr))
                {
                    throw Exception("Group by expression without valid field type", ErrorCodes::COP_BAD_DAG_REQUEST);
                }
                output_field_types.push_back(expr.field_type());
            }
            return true;
        default:
            return false;
    }
}

void DAGQuerySource::analyzeResultFieldTypes()
{
    std::vector<tipb::FieldType> executor_output;
    for (int i = dag_request.executors_size() - 1; i >= 0; i--)
    {
        if (fillExecutorOutputFieldTypes(dag_request.executors(i), executor_output))
            break;
    }
    if (executor_output.empty())
    {
        throw Exception("Do not found result field type for current dag request", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    // tispark assumes that if there is a agg, the output offset is
    // ignored and the request output is the same as the agg's output.
    // todo should always use output offset to re-construct the output field types
    if (hasAggregation())
    {
        result_field_types = std::move(executor_output);
    }
    else
    {
        for (UInt32 i : dag_request.output_offsets())
            result_field_types.push_back(executor_output[i]);
    }
}

} // namespace DB

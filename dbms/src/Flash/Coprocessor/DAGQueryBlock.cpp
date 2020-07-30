#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
#include <Flash/Coprocessor/DAGUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes

class Context;
class TiFlashMetrics;
using TiFlashMetricsPtr = std::shared_ptr<TiFlashMetrics>;

bool isSourceNode(const tipb::Executor * root)
{
    return root->tp() == tipb::ExecType::TypeJoin || root->tp() == tipb::ExecType::TypeTableScan;
}

const static String SOURCE_NAME("source");
const static String SEL_NAME("selection");
const static String AGG_NAME("aggregation");
const static String TOPN_NAME("topN");
const static String LIMIT_NAME("limit");

static void assignOrThrowException(const tipb::Executor ** to, const tipb::Executor * from, const String & name)
{
    if (*to != nullptr)
    {
        throw TiFlashException("Duplicated " + name + " in DAG request", TiFlashErrorRegistry::simpleGet(ErrorClass::Coprocessor, "Internal"));
    }
    *to = from;
}

void collectOutPutFieldTypesFromAgg(std::vector<tipb::FieldType> & field_type, const tipb::Aggregation & agg)
{
    for (auto & expr : agg.agg_func())
    {
        if (!exprHasValidFieldType(expr))
        {
            throw TiFlashException("Agg expression without valid field type", TiFlashErrorRegistry::simpleGet(ErrorClass::Coprocessor, "BadRequest"));
        }
        field_type.push_back(expr.field_type());
    }
    for (auto & expr : agg.group_by())
    {
        if (!exprHasValidFieldType(expr))
        {
            throw TiFlashException("Group by expression without valid field type", TiFlashErrorRegistry::simpleGet(ErrorClass::Coprocessor, "BadRequest"));
        }
        field_type.push_back(expr.field_type());
    }
}

/// construct DAGQueryBlock from a tree struct based executors, which is the
/// format after supporting join in dag request
DAGQueryBlock::DAGQueryBlock(UInt32 id_, const tipb::Executor & root_)
    : id(id_), root(&root_), qb_column_prefix("__QB_" + std::to_string(id_) + "_"), qb_join_subquery_alias(qb_column_prefix + "join")
{
    const tipb::Executor * current = root;
    while (!isSourceNode(current) && current->has_executor_id())
    {
        switch (current->tp())
        {
            case tipb::ExecType::TypeSelection:
                assignOrThrowException(&selection, current, SEL_NAME);
                selection_name = current->executor_id();
                current = &current->selection().child();
                break;
            case tipb::ExecType::TypeAggregation:
            case tipb::ExecType::TypeStreamAgg:
                assignOrThrowException(&aggregation, current, AGG_NAME);
                aggregation_name = current->executor_id();
                collectOutPutFieldTypesFromAgg(output_field_types, current->aggregation());
                current = &current->aggregation().child();
                break;
            case tipb::ExecType::TypeLimit:
                assignOrThrowException(&limitOrTopN, current, LIMIT_NAME);
                limitOrTopN_name = current->executor_id();
                current = &current->limit().child();
                break;
            case tipb::ExecType::TypeTopN:
                assignOrThrowException(&limitOrTopN, current, TOPN_NAME);
                limitOrTopN_name = current->executor_id();
                current = &current->topn().child();
                break;
            case tipb::ExecType::TypeIndexScan:
                throw TiFlashException("Unsupported executor in DAG request: " + current->DebugString(), TiFlashErrorRegistry::simpleGet(ErrorClass::Coprocessor, "Internal"));
            default:
                throw TiFlashException("Should not reach here", TiFlashErrorRegistry::simpleGet(ErrorClass::Coprocessor, "Internal"));
        }
    }

    if (!current->has_executor_id())
        throw TiFlashException("Tree struct based executor must have executor id", TiFlashErrorRegistry::simpleGet(ErrorClass::Coprocessor, "BadRequest"));

    assignOrThrowException(&source, current, SOURCE_NAME);
    source_name = current->executor_id();
    if (current->tp() == tipb::ExecType::TypeJoin)
    {
        if (source->join().children_size() != 2)
            throw TiFlashException("Join executor children size not equal to 2", TiFlashErrorRegistry::simpleGet(ErrorClass::Coprocessor, "BadRequest"));
        children.push_back(std::make_shared<DAGQueryBlock>(id * 2, source->join().children(0)));
        children.push_back(std::make_shared<DAGQueryBlock>(id * 2 + 1, source->join().children(1)));
    }
    fillOutputFieldTypes();
}

/// construct DAGQueryBlock from a list struct based executors, which is the
/// format before supporting join in dag request
DAGQueryBlock::DAGQueryBlock(UInt32 id_, const ::google::protobuf::RepeatedPtrField<tipb::Executor> & executors)
    : id(id_), root(nullptr), qb_column_prefix("__QB_" + std::to_string(id_) + "_"), qb_join_subquery_alias(qb_column_prefix + "join")
{
    for (int i = (int)executors.size() - 1; i >= 0; i--)
    {
        switch (executors[i].tp())
        {
            case tipb::ExecType::TypeTableScan:
                assignOrThrowException(&source, &executors[i], SOURCE_NAME);
                /// use index as the prefix for executor name so when we sort by
                /// the executor name, it will result in the same order as it is
                /// in the dag_request, this is needed when filling executeSummary
                /// in DAGDriver
                source_name = std::to_string(i) + "_tablescan";
                break;
            case tipb::ExecType::TypeSelection:
                assignOrThrowException(&selection, &executors[i], SEL_NAME);
                selection_name = std::to_string(i) + "_selection";
                break;
            case tipb::ExecType::TypeStreamAgg:
            case tipb::ExecType::TypeAggregation:
                assignOrThrowException(&aggregation, &executors[i], AGG_NAME);
                aggregation_name = std::to_string(i) + "_aggregation";
                collectOutPutFieldTypesFromAgg(output_field_types, executors[i].aggregation());
                break;
            case tipb::ExecType::TypeTopN:
                assignOrThrowException(&limitOrTopN, &executors[i], TOPN_NAME);
                limitOrTopN_name = std::to_string(i) + "_limitOrTopN";
                break;
            case tipb::ExecType::TypeLimit:
                assignOrThrowException(&limitOrTopN, &executors[i], LIMIT_NAME);
                limitOrTopN_name = std::to_string(i) + "_limitOrTopN";
                break;
            default:
                throw TiFlashException("Unsupported executor in DAG request: " + executors[i].DebugString(), TiFlashErrorRegistry::simpleGet(ErrorClass::Coprocessor, "Unimplemented"));
        }
    }
    fillOutputFieldTypes();
}

void DAGQueryBlock::fillOutputFieldTypes()
{
    if (source->tp() == tipb::ExecType::TypeJoin)
    {
        if (output_field_types.empty())
        {
            for (auto & field_type : children[0]->output_field_types)
                output_field_types.push_back(field_type);
            for (auto & field_type : children[1]->output_field_types)
                output_field_types.push_back(field_type);
        }
    }
    else
    {
        if (output_field_types.empty())
        {
            for (auto & ci : source->tbl_scan().columns())
            {
                tipb::FieldType field_type;
                field_type.set_tp(ci.tp());
                field_type.set_flag(ci.flag());
                field_type.set_flen(ci.columnlen());
                field_type.set_decimal(ci.decimal());
                output_field_types.push_back(field_type);
            }
        }
    }
}

void DAGQueryBlock::collectAllPossibleChildrenJoinSubqueryAlias(std::unordered_map<UInt32, std::vector<String>> & result)
{
    std::vector<String> all_qb_join_subquery_alias;
    for (auto & child : children)
    {
        child->collectAllPossibleChildrenJoinSubqueryAlias(result);
        all_qb_join_subquery_alias.insert(all_qb_join_subquery_alias.end(), result[child->id].begin(), result[child->id].end());
    }
    all_qb_join_subquery_alias.push_back(qb_join_subquery_alias);
    result[id] = all_qb_join_subquery_alias;
}

} // namespace DB

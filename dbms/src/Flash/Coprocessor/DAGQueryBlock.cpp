#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include "DAGQueryBlock.h"
#include "DAGUtils.h"

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
        throw Exception("Duplicated " + name + " in DAG request");
    }
    *to = from;
}

void collectOutPutFieldTypesFromAgg(std::vector<tipb::FieldType> & field_type, const tipb::Aggregation & agg)
{
    for (auto & expr : agg.agg_func())
    {
        if (!exprHasValidFieldType(expr))
        {
            throw Exception("Agg expression without valid field type", ErrorCodes::COP_BAD_DAG_REQUEST);
        }
        field_type.push_back(expr.field_type());
    }
    for (auto & expr : agg.group_by())
    {
        if (!exprHasValidFieldType(expr))
        {
            throw Exception("Group by expression without valid field type", ErrorCodes::COP_BAD_DAG_REQUEST);
        }
        field_type.push_back(expr.field_type());
    }
}

DAGQueryBlock::DAGQueryBlock(UInt32 id_, const tipb::Executor * root)
: id(id_), qb_column_prefix("___QB_" + std::to_string(id_))
{
    const tipb::Executor * current = root;
    while (!isSourceNode(current))
    {
        switch (current->tp())
        {
            case tipb::ExecType::TypeSelection:
                assignOrThrowException(&selection, current, SEL_NAME);
                current = &current->selection().child();
                break;
            case tipb::ExecType::TypeAggregation:
            case tipb::ExecType::TypeStreamAgg:
                assignOrThrowException(&aggregation, current, AGG_NAME);
                collectOutPutFieldTypesFromAgg(output_field_types, current->aggregation());
                current = &current->aggregation().child();
                break;
            case tipb::ExecType::TypeLimit:
                assignOrThrowException(&limitOrTopN, current, LIMIT_NAME);
                current = &current->limit().child();
                break;
            case tipb::ExecType::TypeTopN:
                assignOrThrowException(&limitOrTopN, current, TOPN_NAME);
                current = &current->topn().child();
                break;
            case tipb::ExecType::TypeIndexScan:
                throw Exception("Unsupported executor in DAG request: " + current->DebugString(), ErrorCodes::NOT_IMPLEMENTED);
            default:
                throw Exception("Should not reach here", ErrorCodes::LOGICAL_ERROR);
        }
    }
    assignOrThrowException(&source, current, SOURCE_NAME);
    if (current->tp() == tipb::ExecType::TypeJoin)
    {
        if (source->join().children_size() != 2)
            throw Exception("Join executor children size not equal to 2field type", ErrorCodes::COP_BAD_DAG_REQUEST);
        children.push_back(std::make_shared<DAGQueryBlock>(id * 2, &source->join().children(0)));
        children.push_back(std::make_shared<DAGQueryBlock>(id * 2 + 1, &source->join().children(1)));
    }
    fillOutputFieldTypes();
}

DAGQueryBlock::DAGQueryBlock(UInt32 id_, std::vector<const tipb::Executor *> & executors, int start_index, int end_index)
: id(id_), qb_column_prefix("___QB_" + std::to_string(id_))
{
    for (int i = end_index; i >= start_index; i--)
    {
        //int build_end_index, build_start_index;
        //int probe_end_index, probe_start_index;
        switch (executors[i]->tp())
        {
            case tipb::ExecType::TypeTableScan:
                assignOrThrowException(&source, executors[i], SOURCE_NAME);
                break;
            case tipb::ExecType::TypeSelection:
                assignOrThrowException(&selection, executors[i], SEL_NAME);
                break;
            case tipb::ExecType::TypeStreamAgg:
            case tipb::ExecType::TypeAggregation:
                assignOrThrowException(&aggregation, executors[i], AGG_NAME);
                collectOutPutFieldTypesFromAgg(output_field_types, executors[i]->aggregation());
                break;
            case tipb::ExecType::TypeTopN:
                assignOrThrowException(&limitOrTopN, executors[i], TOPN_NAME);
                break;
            case tipb::ExecType::TypeLimit:
                assignOrThrowException(&limitOrTopN, executors[i], LIMIT_NAME);
                break;
                /*
            case tipb::ExecType::TypeJoin:
                if (i <= start_index)
                    throw Exception("Join executor without child executor", ErrorCodes::LOGICAL_ERROR);
                if (executors[i - 1] == &(executors[i]->join().build_exec()))
                {
                    build_end_index = i - 1;
                    build_start_index = build_end_index;
                    while (build_start_index >= start_index && executors[build_start_index] != &(executors[i]->join().probe_exec()))
                        build_start_index--;
                    if (build_start_index < start_index)
                        throw Exception("Join executor without child executor", ErrorCodes::LOGICAL_ERROR);
                    probe_end_index = build_start_index;
                    build_start_index++;
                    probe_start_index = start_index;
                }
                else if (executors[i - 1] == &(executors[i]->join().probe_exec()))
                {
                    probe_end_index = i - 1;
                    probe_start_index = probe_end_index;
                    while (probe_start_index >= start_index && executors[probe_start_index] != &(executors[i]->join().build_exec()))
                        probe_start_index--;
                    if (probe_start_index < start_index)
                        throw Exception("Join executor without child executor", ErrorCodes::LOGICAL_ERROR);
                    build_end_index = probe_start_index;
                    probe_start_index++;
                    build_start_index = start_index;
                }
                else
                {
                    throw Exception("Join executor without child executor", ErrorCodes::LOGICAL_ERROR);
                }
                children.push_back(std::make_shared<DAGQueryBlock>(id * 2, executors, probe_start_index, probe_end_index));
                children.push_back(std::make_shared<DAGQueryBlock>(id * 2 + 1, executors, build_start_index, build_end_index));
                // to break the for loop
                i = start_index - 1;
                break;
                 */
            default:
                throw Exception("Unsupported executor in DAG request: " + executors[i]->DebugString(), ErrorCodes::NOT_IMPLEMENTED);
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

} // namespace DB

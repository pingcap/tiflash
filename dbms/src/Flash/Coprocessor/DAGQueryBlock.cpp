#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Common/TiFlashException.h>
#include <Common/TiFlashMetrics.h>
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
    return root->tp() == tipb::ExecType::TypeJoin || root->tp() == tipb::ExecType::TypeTableScan
        || root->tp() == tipb::ExecType::TypeExchangeReceiver || root->tp() == tipb::ExecType::TypeProjection;
}

const static String SOURCE_NAME("source");
const static String SEL_NAME("selection");
const static String AGG_NAME("aggregation");
const static String HAVING_NAME("having");
const static String TOPN_NAME("topN");
const static String LIMIT_NAME("limit");
const static String EXCHANGE_SENDER_NAME("exchange_sender");

static void assignOrThrowException(const tipb::Executor ** to, const tipb::Executor * from, const String & name)
{
    if (*to != nullptr)
    {
        throw TiFlashException("Duplicated " + name + " in DAG request", Errors::Coprocessor::Internal);
    }
    *to = from;
}

void collectOutPutFieldTypesFromAgg(std::vector<tipb::FieldType> & field_type, const tipb::Aggregation & agg)
{
    for (auto & expr : agg.agg_func())
    {
        if (!exprHasValidFieldType(expr))
        {
            throw TiFlashException("Agg expression without valid field type", Errors::Coprocessor::BadRequest);
        }
        field_type.push_back(expr.field_type());
    }
    for (auto & expr : agg.group_by())
    {
        if (!exprHasValidFieldType(expr))
        {
            throw TiFlashException("Group by expression without valid field type", Errors::Coprocessor::BadRequest);
        }
        field_type.push_back(expr.field_type());
    }
}

/// construct DAGQueryBlock from a tree struct based executors, which is the
/// format after supporting join in dag request
DAGQueryBlock::DAGQueryBlock(UInt32 id_, const tipb::Executor & root_, TiFlashMetricsPtr metrics)
    : id(id_), root(&root_), qb_column_prefix("__QB_" + std::to_string(id_) + "_"), qb_join_subquery_alias(qb_column_prefix + "join")
{
    const tipb::Executor * current = root;
    while (!isSourceNode(current) && current->has_executor_id())
    {
        switch (current->tp())
        {
            case tipb::ExecType::TypeSelection:
                if (current->selection().child().tp() == tipb::ExecType::TypeAggregation
                    || current->selection().child().tp() == tipb::ExecType::TypeStreamAgg)
                {
                    /// if the selection is after the aggregation, then it is having, need to be
                    /// executed after aggregation.
                    // todo We should refine the DAGQueryBlock so DAGQueryBlockInterpreter
                    //  could compile the executor in DAG request directly without these preprocess.
                    GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_sel).Increment();
                    assignOrThrowException(&having, current, HAVING_NAME);
                    having_name = current->executor_id();
                }
                else
                {
                    GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_sel).Increment();
                    assignOrThrowException(&selection, current, SEL_NAME);
                    selection_name = current->executor_id();
                }
                current = &current->selection().child();
                break;
            case tipb::ExecType::TypeAggregation:
            case tipb::ExecType::TypeStreamAgg:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_agg).Increment();
                assignOrThrowException(&aggregation, current, AGG_NAME);
                aggregation_name = current->executor_id();
                collectOutPutFieldTypesFromAgg(output_field_types, current->aggregation());
                current = &current->aggregation().child();
                break;
            case tipb::ExecType::TypeLimit:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_limit).Increment();
                assignOrThrowException(&limitOrTopN, current, LIMIT_NAME);
                limitOrTopN_name = current->executor_id();
                current = &current->limit().child();
                break;
            case tipb::ExecType::TypeTopN:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_topn).Increment();
                assignOrThrowException(&limitOrTopN, current, TOPN_NAME);
                limitOrTopN_name = current->executor_id();
                current = &current->topn().child();
                break;
            case tipb::ExecType::TypeExchangeSender:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_exchange_sender).Increment();
                assignOrThrowException(&exchangeSender, current, EXCHANGE_SENDER_NAME);
                exchangeServer_name = current->executor_id();
                current = &current->exchange_sender().child();
                break;
            case tipb::ExecType::TypeIndexScan:
                throw TiFlashException("Unsupported executor in DAG request: " + current->DebugString(), Errors::Coprocessor::Internal);
            default:
                throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
        }
    }

    if (!current->has_executor_id())
        throw TiFlashException("Tree struct based executor must have executor id", Errors::Coprocessor::BadRequest);

    assignOrThrowException(&source, current, SOURCE_NAME);
    source_name = current->executor_id();
    if (current->tp() == tipb::ExecType::TypeJoin)
    {
        if (source->join().children_size() != 2)
            throw TiFlashException("Join executor children size not equal to 2", Errors::Coprocessor::BadRequest);
        GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_join).Increment();
        children.push_back(std::make_shared<DAGQueryBlock>(id * 2, source->join().children(0), metrics));
        children.push_back(std::make_shared<DAGQueryBlock>(id * 2 + 1, source->join().children(1), metrics));
    }
    else if (current->tp() == tipb::ExecType::TypeExchangeReceiver)
    {
        GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_exchange_receiver).Increment();
    }
    else if (current->tp() == tipb::ExecType::TypeProjection)
    {
        GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_projection).Increment();
        children.push_back(std::make_shared<DAGQueryBlock>(id + 1, source->projection().child(), metrics));
    }
    else if (current->tp() == tipb::ExecType::TypeTableScan)
    {
        GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_ts).Increment();
    }
    fillOutputFieldTypes();
}

/// construct DAGQueryBlock from a list struct based executors, which is the
/// format before supporting join in dag request
DAGQueryBlock::DAGQueryBlock(UInt32 id_, const ::google::protobuf::RepeatedPtrField<tipb::Executor> & executors, TiFlashMetricsPtr metrics)
    : id(id_), root(nullptr), qb_column_prefix("__QB_" + std::to_string(id_) + "_"), qb_join_subquery_alias(qb_column_prefix + "join")
{
    for (int i = (int)executors.size() - 1; i >= 0; i--)
    {
        switch (executors[i].tp())
        {
            case tipb::ExecType::TypeTableScan:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_ts).Increment();
                assignOrThrowException(&source, &executors[i], SOURCE_NAME);
                /// use index as the prefix for executor name so when we sort by
                /// the executor name, it will result in the same order as it is
                /// in the dag_request, this is needed when filling execution_summary
                /// in DAGDriver
                if (executors[i].has_executor_id())
                    source_name = executors[i].executor_id();
                else
                    source_name = std::to_string(i) + "_tablescan";
                break;
            case tipb::ExecType::TypeSelection:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_sel).Increment();
                assignOrThrowException(&selection, &executors[i], SEL_NAME);
                if (executors[i].has_executor_id())
                    selection_name = executors[i].executor_id();
                else
                    selection_name = std::to_string(i) + "_selection";
                break;
            case tipb::ExecType::TypeStreamAgg:
            case tipb::ExecType::TypeAggregation:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_agg).Increment();
                assignOrThrowException(&aggregation, &executors[i], AGG_NAME);
                if (executors[i].has_executor_id())
                    aggregation_name = executors[i].executor_id();
                else
                    aggregation_name = std::to_string(i) + "_aggregation";
                collectOutPutFieldTypesFromAgg(output_field_types, executors[i].aggregation());
                break;
            case tipb::ExecType::TypeTopN:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_topn).Increment();
                assignOrThrowException(&limitOrTopN, &executors[i], TOPN_NAME);
                if (executors[i].has_executor_id())
                    limitOrTopN_name = executors[i].executor_id();
                else
                    limitOrTopN_name = std::to_string(i) + "_limitOrTopN";
                break;
            case tipb::ExecType::TypeLimit:
                GET_METRIC(metrics, tiflash_coprocessor_executor_count, type_limit).Increment();
                assignOrThrowException(&limitOrTopN, &executors[i], LIMIT_NAME);
                if (executors[i].has_executor_id())
                    limitOrTopN_name = executors[i].executor_id();
                else
                    limitOrTopN_name = std::to_string(i) + "_limitOrTopN";
                break;
            default:
                throw TiFlashException(
                    "Unsupported executor in DAG request: " + executors[i].DebugString(), Errors::Coprocessor::Unimplemented);
        }
    }
    fillOutputFieldTypes();
}

void DAGQueryBlock::fillOutputFieldTypes()
{
    /// the top block has exchangeSender, which decides the output fields, keeping the same with exchangeReceiver
    if (exchangeSender != nullptr && exchangeSender->has_exchange_sender() && !exchangeSender->exchange_sender().all_field_types().empty())
    {
        output_field_types.clear();
        for (auto & field_type : exchangeSender->exchange_sender().all_field_types())
        {
            output_field_types.push_back(field_type);
        }
        return;
    }
    /// the non-top block
    if (!output_field_types.empty())
    {
        return;
    }
    if (source->tp() == tipb::ExecType::TypeJoin)
    {
        for (auto & field_type : children[0]->output_field_types)
        {
            if (source->join().join_type() == tipb::JoinType::TypeRightOuterJoin)
            {
                /// the type of left column for right join is always nullable
                auto updated_field_type = field_type;
                updated_field_type.set_flag((UInt32)updated_field_type.flag() & (~(UInt32)TiDB::ColumnFlagNotNull));
                output_field_types.push_back(updated_field_type);
            }
            else
            {
                output_field_types.push_back(field_type);
            }
        }
        if (source->join().join_type() != tipb::JoinType::TypeSemiJoin && source->join().join_type() != tipb::JoinType::TypeAntiSemiJoin)
        {
            /// for semi/anti semi join, the right table column is ignored
            for (auto & field_type : children[1]->output_field_types)
            {
                if (source->join().join_type() == tipb::JoinType::TypeLeftOuterJoin)
                {
                    /// the type of right column for left join is always nullable
                    auto updated_field_type = field_type;
                    updated_field_type.set_flag(updated_field_type.flag() & (~(UInt32)TiDB::ColumnFlagNotNull));
                    output_field_types.push_back(updated_field_type);
                }
                else
                {
                    output_field_types.push_back(field_type);
                }
            }
        }
    }
    else if (source->tp() == tipb::ExecType::TypeExchangeReceiver)
    {
        for (auto & field_type : source->exchange_receiver().field_types())
        {
            output_field_types.push_back(field_type);
        }
    }
    else if (source->tp() == tipb::ExecType::TypeProjection)
    {
        for (auto & expr : source->projection().exprs())
        {
            output_field_types.push_back(expr.field_type());
        }
    }
    else
    {
        for (auto & ci : source->tbl_scan().columns())
        {
            tipb::FieldType field_type;
            field_type.set_tp(ci.tp());
            field_type.set_flag(ci.flag());
            field_type.set_flen(ci.columnlen());
            field_type.set_decimal(ci.decimal());
            for (const auto & elem : ci.elems())
            {
                field_type.add_elems(elem);
            }
            output_field_types.push_back(field_type);
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

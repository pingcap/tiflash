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

bool isSourceNode(const tipb::Executor * root)
{
    return root->tp() == tipb::ExecType::TypeJoin || root->tp() == tipb::ExecType::TypeTableScan
        || root->tp() == tipb::ExecType::TypeExchangeReceiver || root->tp() == tipb::ExecType::TypeProjection;
}

const static String SOURCE_NAME("source");
const static String SEL_NAME("selection");
const static String AGG_NAME("aggregation");
const static String WINDOW_NAME("window");
const static String WINDOW_SORT_NAME("window_sort");
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

void collectOutPutFieldTypesFromWindow(std::vector<tipb::FieldType> & field_type, const tipb::Window & window)
{
    for (const auto & expr : window.func_desc())
    {
        if (!exprHasValidFieldType(expr))
        {
            throw TiFlashException("Agg expression without valid field type", Errors::Coprocessor::BadRequest);
        }
        field_type.push_back(expr.field_type());
    }
}

/// construct DAGQueryBlock from a tree struct based executors, which is the
/// format after supporting join in dag request
DAGQueryBlock::DAGQueryBlock(const tipb::Executor & root_, QueryBlockIDGenerator & id_generator)
    : id(id_generator.nextBlockID())
    , root(&root_)
    , qb_column_prefix("__QB_" + std::to_string(id) + "_")
{
    const tipb::Executor * current = root;
    String window_name;
    String window_sort_name;
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
                GET_METRIC(tiflash_coprocessor_executor_count, type_sel).Increment();
                assignOrThrowException(&having, current, HAVING_NAME);
                having_name = current->executor_id();
            }
            else
            {
                GET_METRIC(tiflash_coprocessor_executor_count, type_sel).Increment();
                assignOrThrowException(&selection, current, SEL_NAME);
                selection_name = current->executor_id();
            }
            current = &current->selection().child();
            break;
        case tipb::ExecType::TypeAggregation:
        case tipb::ExecType::TypeStreamAgg:
            GET_METRIC(tiflash_coprocessor_executor_count, type_agg).Increment();
            assignOrThrowException(&aggregation, current, AGG_NAME);
            aggregation_name = current->executor_id();
            current = &current->aggregation().child();
            break;
        case tipb::ExecType::TypeWindow:
            GET_METRIC(tiflash_coprocessor_executor_count, type_window).Increment();
            window_name = current->executor_id();
            //            assignOrThrowException(&window, current, WINDOW_NAME);
            windows.insert({window_name, current});
            window_op_list.push_back(window_name);
            collectOutPutFieldTypesFromWindow(output_field_types, current->window());
            current = &current->window().child();
            break;
        case tipb::ExecType::TypeSort:
            // only isPartialSort = ture is for window function sort.
            if (!current->sort().ispartialsort())
                break;
            GET_METRIC(tiflash_coprocessor_executor_count, type_window).Increment();
            window_sort_name = current->executor_id();
            //            assignOrThrowException(&window_sort, current, WINDOW_SORT_NAME);
            window_sorts.insert({window_sort_name, current});
            window_op_list.push_back(window_sort_name);
            current = &current->sort().child();
            break;
        case tipb::ExecType::TypeLimit:
            GET_METRIC(tiflash_coprocessor_executor_count, type_limit).Increment();
            assignOrThrowException(&limit_or_topn, current, LIMIT_NAME);
            limit_or_topn_name = current->executor_id();
            current = &current->limit().child();
            break;
        case tipb::ExecType::TypeTopN:
            GET_METRIC(tiflash_coprocessor_executor_count, type_topn).Increment();
            assignOrThrowException(&limit_or_topn, current, TOPN_NAME);
            limit_or_topn_name = current->executor_id();
            current = &current->topn().child();
            break;
        case tipb::ExecType::TypeExchangeSender:
            GET_METRIC(tiflash_coprocessor_executor_count, type_exchange_sender).Increment();
            assignOrThrowException(&exchange_sender, current, EXCHANGE_SENDER_NAME);
            exchange_sender_name = current->executor_id();
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
        GET_METRIC(tiflash_coprocessor_executor_count, type_join).Increment();
        children.push_back(std::make_shared<DAGQueryBlock>(source->join().children(0), id_generator));
        children.push_back(std::make_shared<DAGQueryBlock>(source->join().children(1), id_generator));
    }
    else if (current->tp() == tipb::ExecType::TypeExchangeReceiver)
    {
        GET_METRIC(tiflash_coprocessor_executor_count, type_exchange_receiver).Increment();
    }
    else if (current->tp() == tipb::ExecType::TypeProjection)
    {
        GET_METRIC(tiflash_coprocessor_executor_count, type_projection).Increment();
        children.push_back(std::make_shared<DAGQueryBlock>(source->projection().child(), id_generator));
    }
    else if (current->tp() == tipb::ExecType::TypeTableScan)
    {
        GET_METRIC(tiflash_coprocessor_executor_count, type_ts).Increment();
    }
}

/// construct DAGQueryBlock from a list struct based executors, which is the
/// format before supporting join in dag request
DAGQueryBlock::DAGQueryBlock(UInt32 id_, const ::google::protobuf::RepeatedPtrField<tipb::Executor> & executors)
    : id(id_)
    , root(nullptr)
    , qb_column_prefix("__QB_" + std::to_string(id_) + "_")
{
    String window_name;
    String window_sort_name;
    for (int i = executors.size() - 1; i >= 0; i--)
    {
        switch (executors[i].tp())
        {
        case tipb::ExecType::TypeTableScan:
            GET_METRIC(tiflash_coprocessor_executor_count, type_ts).Increment();
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
            GET_METRIC(tiflash_coprocessor_executor_count, type_sel).Increment();
            assignOrThrowException(&selection, &executors[i], SEL_NAME);
            if (executors[i].has_executor_id())
                selection_name = executors[i].executor_id();
            else
                selection_name = std::to_string(i) + "_selection";
            break;
        case tipb::ExecType::TypeStreamAgg:
        case tipb::ExecType::TypeAggregation:
            GET_METRIC(tiflash_coprocessor_executor_count, type_agg).Increment();
            assignOrThrowException(&aggregation, &executors[i], AGG_NAME);
            if (executors[i].has_executor_id())
                aggregation_name = executors[i].executor_id();
            else
                aggregation_name = std::to_string(i) + "_aggregation";
            break;
        case tipb::ExecType::TypeWindow:
            GET_METRIC(tiflash_coprocessor_executor_count, type_window).Increment();
            //            assignOrThrowException(&window, &executors[i], WINDOW_NAME);
            if (executors[i].has_executor_id())
                window_name = executors[i].executor_id();
            else
                window_name = std::to_string(i) + "_window";
            windows.insert({window_name, &executors[i]});
            window_op_list.push_back(window_name);
            collectOutPutFieldTypesFromWindow(output_field_types, executors[i].window());
            break;
        case tipb::ExecType::TypeSort:
            if (!executors[i].sort().ispartialsort())
                break;
            GET_METRIC(tiflash_coprocessor_executor_count, type_window_sort).Increment();
            //            assignOrThrowException(&window_sort, &executors[i], WINDOW_SORT_NAME);
            if (executors[i].has_executor_id())
                window_sort_name = executors[i].executor_id();
            else
                window_sort_name = std::to_string(i) + "_window";
            windows.insert({window_sort_name, &executors[i]});
            window_op_list.push_back(window_sort_name);
            break;
        case tipb::ExecType::TypeTopN:
            GET_METRIC(tiflash_coprocessor_executor_count, type_topn).Increment();
            assignOrThrowException(&limit_or_topn, &executors[i], TOPN_NAME);
            if (executors[i].has_executor_id())
                limit_or_topn_name = executors[i].executor_id();
            else
                limit_or_topn_name = std::to_string(i) + "_limitOrTopN";
            break;
        case tipb::ExecType::TypeLimit:
            GET_METRIC(tiflash_coprocessor_executor_count, type_limit).Increment();
            assignOrThrowException(&limit_or_topn, &executors[i], LIMIT_NAME);
            if (executors[i].has_executor_id())
                limit_or_topn_name = executors[i].executor_id();
            else
                limit_or_topn_name = std::to_string(i) + "_limitOrTopN";
            break;
        default:
            throw TiFlashException(
                "Unsupported executor in DAG request: " + executors[i].DebugString(),
                Errors::Coprocessor::Unimplemented);
        }
    }
}

} // namespace DB

#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Flash/Plan/toPlan.h>
#include <Parsers/makeDummyQuery.h>

namespace DB
{
namespace ErrorCodes
{
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes

DAGQuerySource::DAGQuerySource(Context & context_)
    : context(context_)
{
    const tipb::DAGRequest & dag_request = *getDAGContext().dag_request;
    PlanPtr plan = toPlan(dag_request);
    if (dag_request.has_root_executor())
    {
        QueryBlockIDGenerator id_generator;
        root_query_block = std::make_shared<DAGQueryBlock>(dag_request.root_executor(), id_generator);
    }
    else
    {
        root_query_block = std::make_shared<DAGQueryBlock>(1, dag_request.executors());
    }
    root_query_block->collectAllPossibleChildrenJoinSubqueryAlias(getDAGContext().getQBIdToJoinAliasMap());
    for (Int32 i : dag_request.output_offsets())
        root_query_block->output_offsets.push_back(i);
    for (UInt32 i : dag_request.output_offsets())
    {
        if (unlikely(i >= root_query_block->output_field_types.size()))
            throw TiFlashException(std::string(__PRETTY_FUNCTION__) + ": Invalid output offset(schema has "
                                       + std::to_string(root_query_block->output_field_types.size()) + " columns, access index " + std::to_string(i),
                                   Errors::Coprocessor::BadRequest);
        getDAGContext().result_field_types.push_back(root_query_block->output_field_types[i]);
    }
    auto encode_type = analyzeDAGEncodeType(getDAGContext());
    getDAGContext().encode_type = encode_type;
    getDAGContext().keep_session_timezone_info = encode_type == tipb::EncodeType::TypeChunk || encode_type == tipb::EncodeType::TypeCHBlock;
}

std::tuple<std::string, ASTPtr> DAGQuerySource::parse(size_t)
{
    // this is a WAR to avoid NPE when the MergeTreeDataSelectExecutor trying
    // to extract key range of the query.
    // todo find a way to enable key range extraction for dag query
    return {getDAGContext().dag_request->DebugString(), makeDummyQuery()};
}

String DAGQuerySource::str(size_t)
{
    return getDAGContext().dag_request->DebugString();
}

std::unique_ptr<IInterpreter> DAGQuerySource::interpreter(Context &, QueryProcessingStage::Enum)
{
    return std::make_unique<InterpreterDAG>(context, *this);
}

} // namespace DB

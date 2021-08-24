#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/makeDummyQuery.h>
#include <Parsers/parseQuery.h>

#include <deque>

namespace DB
{

namespace ErrorCodes
{
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes

DAGQuerySource::DAGQuerySource(Context & context_, const RegionInfoMap & regions_, const RegionInfoList & retry_regions_,
    const tipb::DAGRequest & dag_request_, const bool is_batch_cop_)
    : context(context_), regions(regions_), retry_regions(retry_regions_), dag_request(dag_request_), is_batch_cop(is_batch_cop_)
{
    if (dag_request.has_root_executor())
    {
        root_query_block = std::make_shared<DAGQueryBlock>(1, dag_request.root_executor());
    }
    else
    {
        root_query_block = std::make_shared<DAGQueryBlock>(1, dag_request.executors());
    }
    root_query_block->collectAllPossibleChildrenJoinSubqueryAlias(context.getDAGContext()->getQBIdToJoinAliasMap());
    for (Int32 i : dag_request.output_offsets())
        root_query_block->output_offsets.push_back(i);
    for (UInt32 i : dag_request.output_offsets())
    {
        if (unlikely(i >= root_query_block->output_field_types.size()))
            throw TiFlashException(std::string(__PRETTY_FUNCTION__) + ": Invalid output offset(schema has "
                    + std::to_string(root_query_block->output_field_types.size()) + " columns, access index " + std::to_string(i),
                Errors::Coprocessor::BadRequest);
        result_field_types.push_back(root_query_block->output_field_types[i]);
    }
    analyzeDAGEncodeType();
}

void DAGQuerySource::analyzeDAGEncodeType()
{
    if (getDAGContext().isMPPTask() && !getDAGContext().isRootMPPTask())
    {
        /// always use CHBlock encode type for data exchange between TiFlash nodes
        encode_type = tipb::EncodeType::TypeCHBlock;
        return;
    }
    encode_type = dag_request.encode_type();
    if (isUnsupportedEncodeType(getResultFieldTypes(), encode_type))
        encode_type = tipb::EncodeType::TypeDefault;
    if (encode_type == tipb::EncodeType::TypeChunk && dag_request.has_chunk_memory_layout()
        && dag_request.chunk_memory_layout().has_endian() && dag_request.chunk_memory_layout().endian() == tipb::Endian::BigEndian)
        // todo support BigEndian encode for chunk encode type
        encode_type = tipb::EncodeType::TypeDefault;
}

std::tuple<std::string, ASTPtr> DAGQuerySource::parse(size_t)
{
    // this is a WAR to avoid NPE when the MergeTreeDataSelectExecutor trying
    // to extract key range of the query.
    // todo find a way to enable key range extraction for dag query
    return {dag_request.DebugString(), makeDummyQuery()};
}

String DAGQuerySource::str(size_t) { return dag_request.DebugString(); }

std::unique_ptr<IInterpreter> DAGQuerySource::interpreter(Context &, QueryProcessingStage::Enum)
{
    return std::make_unique<InterpreterDAG>(context, *this, mpp_task_log);
}

std::string DAGQuerySource::getExecutorNames() const
{
    std::string names("");
    std::deque<std::shared_ptr<DAGQueryBlock>> dq;
    dq.push_back(root_query_block);

    while (!dq.empty())
    {
        std::shared_ptr<DAGQueryBlock> dag_query_block = dq.front();
        dq.pop_front();

        if (dag_query_block == nullptr)
            continue;

        if (dag_query_block->source != nullptr)
            names += "<" + dag_query_block->source_name + "> ";
        if (dag_query_block->selection != nullptr)
            names += "<" + dag_query_block->selection_name + "> ";
        if (dag_query_block->aggregation != nullptr)
            names += "<" + dag_query_block->aggregation_name + "> ";
        if (dag_query_block->having != nullptr)
            names += "<" + dag_query_block->having_name + "> ";
        if (dag_query_block->limitOrTopN != nullptr)
            names += "<" + dag_query_block->limitOrTopN_name + "> ";
        if (dag_query_block->exchangeSender != nullptr)
            names += "<" + dag_query_block->exchangeServer_name + "> ";

        for (auto child : dag_query_block->children)
            dq.push_back(child);
    }

    return names;
}

} // namespace DB

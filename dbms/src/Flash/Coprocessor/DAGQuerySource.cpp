#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Parsers/makeDummyQuery.h>
#include <fmt/core.h>

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
    for (UInt32 i : dag_request.output_offsets())
    {
        root_query_block->output_offsets.push_back(i);
        if (unlikely(i >= root_query_block->output_field_types.size()))
            throw TiFlashException(
                fmt::format("{}: Invalid output offset(schema has {} columns, access index {}", __PRETTY_FUNCTION__, root_query_block->output_field_types.size(), i),
                Errors::Coprocessor::BadRequest);
        getDAGContext().result_field_types.push_back(root_query_block->output_field_types[i]);
    }
    auto encode_type = analyzeDAGEncodeType();
    getDAGContext().encode_type = encode_type;
    getDAGContext().keep_session_timezone_info = encode_type == tipb::EncodeType::TypeChunk || encode_type == tipb::EncodeType::TypeCHBlock;
}

tipb::EncodeType DAGQuerySource::analyzeDAGEncodeType() const
{
    const tipb::DAGRequest & dag_request = *getDAGContext().dag_request;
    const tipb::EncodeType encode_type = dag_request.encode_type();
    if (getDAGContext().isMPPTask() && !getDAGContext().isRootMPPTask())
    {
        /// always use CHBlock encode type for data exchange between TiFlash nodes
        return tipb::EncodeType::TypeCHBlock;
    }
    if (dag_request.has_force_encode_type() && dag_request.force_encode_type())
    {
        assert(encode_type == tipb::EncodeType::TypeCHBlock);
        return encode_type;
    }
    if (isUnsupportedEncodeType(getDAGContext().result_field_types, encode_type))
        return tipb::EncodeType::TypeDefault;
    if (encode_type == tipb::EncodeType::TypeChunk && dag_request.has_chunk_memory_layout()
        && dag_request.chunk_memory_layout().has_endian() && dag_request.chunk_memory_layout().endian() == tipb::Endian::BigEndian)
        // todo support BigEndian encode for chunk encode type
        return tipb::EncodeType::TypeDefault;
    return encode_type;
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

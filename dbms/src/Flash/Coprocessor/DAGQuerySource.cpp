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

DAGQuerySource::DAGQuerySource(Context & context_, const std::unordered_map<RegionID, RegionInfo> & regions_,
    const tipb::DAGRequest & dag_request_, const bool is_batch_cop_)
    : context(context_),
      regions(regions_),
      dag_request(dag_request_),
      is_batch_cop(is_batch_cop_)
{
    if (dag_request.has_root_executor())
    {
        root_query_block = std::make_shared<DAGQueryBlock>(1, dag_request.root_executor(), context.getTiFlashMetrics());
    }
    else
    {
        root_query_block = std::make_shared<DAGQueryBlock>(1, dag_request.executors(), context.getTiFlashMetrics());
    }
    root_query_block->collectAllPossibleChildrenJoinSubqueryAlias(context.getDAGContext()->getQBIdToJoinAliasMap());
    for (Int32 i : dag_request.output_offsets())
        root_query_block->output_offsets.push_back(i);
    if (root_query_block->aggregation != nullptr)
    {
        for (auto & field_type : root_query_block->output_field_types)
            result_field_types.push_back(field_type);
    }
    else
    {
        for (UInt32 i : dag_request.output_offsets())
        {
            result_field_types.push_back(root_query_block->output_field_types[i]);
        }
    }
    analyzeDAGEncodeType();
}

void DAGQuerySource::analyzeDAGEncodeType()
{
    encode_type = dag_request.encode_type();
    if (isUnsupportedEncodeType(getResultFieldTypes(), encode_type))
        encode_type = tipb::EncodeType::TypeDefault;
    if (encode_type == tipb::EncodeType::TypeChunk && dag_request.has_chunk_memory_layout()
        && dag_request.chunk_memory_layout().has_endian() && dag_request.chunk_memory_layout().endian() == tipb::Endian::BigEndian)
        // todo support BigEndian encode for chunk encode type
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

} // namespace DB

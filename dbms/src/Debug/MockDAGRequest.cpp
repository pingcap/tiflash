#include <Debug/MockDAGRequest.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
std::shared_ptr<tipb::DAGRequest> TiPBDAGRequestBuilder::build(Context & context)
{
    DAGProperties properties;
    MPPInfo mpp_info(properties.start_ts, -1, -1, {}, {});

    std::shared_ptr<tipb::DAGRequest> dag_request_ptr = std::make_shared<tipb::DAGRequest>();
    tipb::DAGRequest & dag_request = *dag_request_ptr;
    dag_request.set_time_zone_name(properties.tz_name);
    dag_request.set_time_zone_offset(properties.tz_offset);
    dag_request.set_flags(dag_request.flags() | (1u << 1u /* TRUNCATE_AS_WARNING */) | (1u << 6u /* OVERFLOW_AS_WARNING */));

    if (properties.encode_type == "chunk")
        dag_request.set_encode_type(tipb::EncodeType::TypeChunk);
    else if (properties.encode_type == "chblock")
        dag_request.set_encode_type(tipb::EncodeType::TypeCHBlock);
    else
        dag_request.set_encode_type(tipb::EncodeType::TypeDefault);

    for (size_t i = 0; i < root->output_schema.size(); ++i)
        dag_request.add_output_offsets(i);
    auto * root_tipb_executor = dag_request.mutable_root_executor();

    root->toTiPBExecutor(root_tipb_executor, properties.collator, mpp_info, context);
    return dag_request_ptr;
}

TiPBDAGRequestBuilder & TiPBDAGRequestBuilder::mockTable(String db, String table, const std::vector<std::pair<String, TiDB::TP>> & columns)
{
    assert(!columns.empty());
    TableInfo table_info;
    table_info.name = db + "." + table;
    for (const auto & column : columns)
    {
        TiDB::ColumnInfo ret;
        ret.tp = column.second;
        ret.name = column.first;
        table_info.columns.push_back(std::move(ret));
    }
    String empty_alias;
    root = compileTableScan(getExecutorIndex(), table_info, empty_alias, false);
    return *this;
}

TiPBDAGRequestBuilder & TiPBDAGRequestBuilder::filter(ASTPtr filter_expr)
{
    assert(root);
    root = compileSelection(root, getExecutorIndex(), filter_expr);
    return *this;
}

TiPBDAGRequestBuilder & TiPBDAGRequestBuilder::limit(ASTPtr limit_expr)
{
    assert(root);
    root = compileLimit(root, getExecutorIndex(), limit_expr);
    return *this;
}

TiPBDAGRequestBuilder & TiPBDAGRequestBuilder::topN(ASTPtr order_exprs, ASTPtr limit_expr)
{
    assert(root);
    root = compileTopN(root, getExecutorIndex(), order_exprs, limit_expr);
    return *this;
}

TiPBDAGRequestBuilder & TiPBDAGRequestBuilder::project(ASTPtr select_list)
{
    assert(root);
    root = compileProject(root, getExecutorIndex(), select_list);
    return *this;
}

TiPBDAGRequestBuilder & TiPBDAGRequestBuilder::join(const TiPBDAGRequestBuilder & right, ASTPtr on_expr)
{
    assert(root);
    assert(right.root);
    auto join_ast = std::make_shared<ASTTableJoin>();
    join_ast->using_expression_list = on_expr;
    join_ast->strictness = ASTTableJoin::Strictness::All;
    root = compileJoin(getExecutorIndex(), root, right.root, join_ast);
    return *this;
}

AstExprBuilder & AstExprBuilder::appendColumnRef(const String & column_name)
{
    vec.push_back(std::make_shared<ASTIdentifier>(column_name));
    return *this;
}

AstExprBuilder & AstExprBuilder::appendLiteral(const Field & field)
{
    vec.push_back(std::make_shared<ASTLiteral>(field));
    return *this;
}

AstExprBuilder & AstExprBuilder::appendOrderByItem(const String & column_name, bool asc)
{
    int direction = asc ? 1 : -1;
    ASTPtr locale_node;
    auto order_by_item = std::make_shared<ASTOrderByElement>(direction, direction, false, locale_node);
    order_by_item->children.push_back(std::make_shared<ASTIdentifier>(column_name));
    vec.push_back(order_by_item);
    return *this;
}

AstExprBuilder & AstExprBuilder::appendList()
{
    auto exp_list = std::make_shared<ASTExpressionList>();
    for (const auto & ast_ptr : vec)
        exp_list->children.push_back(ast_ptr);
    vec.clear();
    vec.push_back(exp_list);
    return *this;
}

AstExprBuilder & AstExprBuilder::appendFunction(const String & func_name)
{
    appendList();
    auto func = std::make_shared<ASTFunction>();
    func->name = func_name;
    ASTPtr exp_list = vec.back();
    func->arguments = exp_list;
    func->children.push_back(func->arguments);
    vec.clear();
    vec.push_back(func);
    return *this;
}

ASTPtr AstExprBuilder::build()
{
    assert(vec.size() == 1);
    ASTPtr ret = vec.back();
    vec.clear();
    return ret;
}
} // namespace DB
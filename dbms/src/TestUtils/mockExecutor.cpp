#include <Common/TypeList.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <TestUtils/mockExecutor.h>
#include <common/types.h>

#include <cassert>
#include <memory>

namespace DB
{
ASTPtr buildColumn(const String & column_name)
{
    return std::make_shared<ASTIdentifier>(column_name);
}

ASTPtr buildLiteral(const Field & field)
{
    return std::make_shared<ASTLiteral>(field);
}

ASTPtr buildFunction(MockAsts exprs, const String & name)
{
    auto func = std::make_shared<ASTFunction>();
    func->name = name;
    auto expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & expr : exprs)
        expr_list->children.push_back(expr);
    func->arguments = expr_list;
    func->children.push_back(func->arguments);
    return func;
}

ASTPtr buildOrderByItemList(MockOrderByItems order_by_items)
{
    std::vector<ASTPtr> vec;
    for (auto item : order_by_items)
    {
        int direction = item.second ? 1 : -1;
        ASTPtr locale_node;
        auto order_by_item = std::make_shared<ASTOrderByElement>(direction, direction, false, locale_node);
        order_by_item->children.push_back(std::make_shared<ASTIdentifier>(item.first));
        vec.push_back(order_by_item);
    }
    auto exp_list = std::make_shared<ASTExpressionList>();
    for (const auto & ast_ptr : vec)
        exp_list->children.push_back(ast_ptr);
    return exp_list;
}

void DAGRequestBuilder::initDAGRequest(tipb::DAGRequest & dag_request)
{
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
}

std::shared_ptr<tipb::DAGRequest> DAGRequestBuilder::build(Context & context)
{
    MPPInfo mpp_info(properties.start_ts, -1, -1, {}, {});
    std::shared_ptr<tipb::DAGRequest> dag_request_ptr = std::make_shared<tipb::DAGRequest>();
    tipb::DAGRequest & dag_request = *dag_request_ptr;
    initDAGRequest(dag_request);
    root->toTiPBExecutor(dag_request.mutable_root_executor(), properties.collator, mpp_info, context);

    return dag_request_ptr;
}

DAGRequestBuilder & DAGRequestBuilder::mockTable(const String & db, const String & table, const MockColumnInfos & columns)
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

DAGRequestBuilder & DAGRequestBuilder::mockTable(const MockTableName & name, const std::vector<std::pair<String, TiDB::TP>> & columns)
{
    return mockTable(name.first, name.second, columns);
}

DAGRequestBuilder & DAGRequestBuilder::filter(ASTPtr filter_expr)
{
    assert(root);
    root = compileSelection(root, getExecutorIndex(), filter_expr);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::limit(int limit)
{
    assert(root);
    root = compileLimit(root, getExecutorIndex(), buildLiteral(Field(static_cast<UInt64>(limit))));
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::limit(ASTPtr limit_expr)
{
    assert(root);
    root = compileLimit(root, getExecutorIndex(), limit_expr);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::topN(ASTPtr order_exprs, ASTPtr limit_expr)
{
    assert(root);
    root = compileTopN(root, getExecutorIndex(), order_exprs, limit_expr);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::topN(const String & col_name, bool desc, int limit)
{
    assert(root);
    root = compileTopN(root, getExecutorIndex(), buildOrderByItemList({{col_name, desc}}), buildLiteral(Field(static_cast<UInt64>(limit))));
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::topN(MockOrderByItems order_by_items, int limit)
{
    return topN(order_by_items, buildLiteral(Field(static_cast<UInt64>(limit))));
}

DAGRequestBuilder & DAGRequestBuilder::topN(MockOrderByItems order_by_items, ASTPtr limit_expr)
{
    assert(root);
    root = compileTopN(root, getExecutorIndex(), buildOrderByItemList(order_by_items), limit_expr);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::project(const String & col_name)
{
    assert(root);
    root = compileProject(root, getExecutorIndex(), buildColumn(col_name));
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::project(MockAsts exprs)
{
    assert(root);
    auto exp_list = std::make_shared<ASTExpressionList>();
    for (const auto & expr : exprs)
        exp_list->children.push_back(expr);
    root = compileProject(root, getExecutorIndex(), exp_list);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::project(MockColumnNames col_names)
{
    assert(root);
    auto exp_list = std::make_shared<ASTExpressionList>();
    for (const auto & name : col_names)
        exp_list->children.push_back(col(name));

    root = compileProject(root, getExecutorIndex(), exp_list);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::join(const DAGRequestBuilder & right, ASTPtr using_expr_list)
{
    assert(root);
    assert(right.root);
    auto join_ast = std::make_shared<ASTTableJoin>();
    join_ast->using_expression_list = using_expr_list;
    join_ast->strictness = ASTTableJoin::Strictness::All;
    root = compileJoin(getExecutorIndex(), root, right.root, join_ast);
    return *this;
}

} // namespace DB
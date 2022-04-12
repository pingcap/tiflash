#pragma once

#include <Debug/astToExecutor.h>
#include <Interpreters/Context.h>

#include <initializer_list>
#include <utility>

namespace DB
{
// <name, type>
using MockColumnInfo = std::pair<String, TiDB::TP>;
using MockTableName = std::pair<String, String>;
using MockOrderByItem = std::pair<String, bool>;
using MockOrderByItems = std::initializer_list<MockOrderByItem>;
using MockColumnNames = std::initializer_list<String>;

class DAGRequestBuilder
{
public:
    static size_t & getExecutorIndex()
    {
        thread_local size_t executor_index = 0;
        return executor_index;
    }

    std::shared_ptr<tipb::DAGRequest> build(Context & context);

    DAGRequestBuilder & mockTable(String db, String table, const std::vector<MockColumnInfo> & columns);
    DAGRequestBuilder & mockTable(MockTableName name, const std::vector<MockColumnInfo> & columns);

    DAGRequestBuilder & filter(ASTPtr filter_expr);

    DAGRequestBuilder & limit(int limit);
    DAGRequestBuilder & limit(ASTPtr limit_expr);

    DAGRequestBuilder & topN(ASTPtr order_exprs, ASTPtr limit_expr);
    DAGRequestBuilder & topN(const String & col_name, bool desc, int limit);
    DAGRequestBuilder & topN(MockOrderByItems order_by_items, int limit);
    DAGRequestBuilder & topN(MockOrderByItems order_by_items, ASTPtr limit_expr);

    DAGRequestBuilder & project(String col_name);
    DAGRequestBuilder & project(std::initializer_list<ASTPtr> cols);
    DAGRequestBuilder & project(MockColumnNames col_names);

    // only support inner join
    // TODO support more joins
    DAGRequestBuilder & join(const DAGRequestBuilder & right, ASTPtr using_expr_list);

private:
    void initDAGRequest(tipb::DAGRequest & dag_request);

    ExecutorPtr root;
    DAGProperties properties;
};

ASTPtr buildColumn(const String & column_name);
ASTPtr buildLiteral(const Field & field);
ASTPtr buildFunction(std::initializer_list<ASTPtr> exprs, const String & name);
ASTPtr buildOrderByItemList(std::initializer_list<std::pair<String, bool>> order_by_items);


#define col(name) buildColumn((name))
#define lit(field) buildLiteral((field))
#define eq(expr1, expr2) buildFunction({(expr1), (expr2)}, "equals")
#define Not_eq(expr1, expr2) buildFunction({(expr1), (expr2)}, "notEquals")
#define lt(expr1, expr2) buildFunction({(expr1), (expr2)}, "less")
#define gt(expr1, expr2) buildFunction({(expr1), (expr2)}, "greater")
#define And(expr1, expr2) buildFunction({(expr1), (expr2)}, "and")
#define Or(expr1, expr2) buildFunction({(expr1), (expr2)}, "or")
#define NOT(expr) buildFunction({expr1}, "not")

} // namespace DB
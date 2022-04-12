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
class AstExprBuilder;


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

    DAGRequestBuilder & filter(AstExprBuilder filter_expr);
    DAGRequestBuilder & filter(ASTPtr filter_expr);

    DAGRequestBuilder & limit(int limit);

    DAGRequestBuilder & topN(ASTPtr order_exprs, ASTPtr limit_expr);
    DAGRequestBuilder & topN(String col_name, bool desc, int limit);
    DAGRequestBuilder & topN(std::initializer_list<String> cols, bool desc, int limit);

    DAGRequestBuilder & project(String col_name);
    DAGRequestBuilder & project(std::initializer_list<ASTPtr> cols);
    DAGRequestBuilder & project(std::initializer_list<String> cols);

    // only support inner join
    // TODO support more joins
    DAGRequestBuilder & join(const DAGRequestBuilder & right, ASTPtr using_expr_list);

private:
    ExecutorPtr root;
};

class AstExprBuilder
{
public:
    AstExprBuilder & appendColumnRef(const String & column_name);

    AstExprBuilder & appendLiteral(const Field & field);

    AstExprBuilder & appendOrderByItem(const String & column_name, bool asc);

    AstExprBuilder & appendList();

    AstExprBuilder & appendAlias(String alias);

    AstExprBuilder & appendFunction(const String & func_name);

    ASTPtr build();

private:
    std::vector<ASTPtr> vec;
};

ASTPtr buildFunction(std::initializer_list<ASTPtr> exprs, String name);

#define COL(name) AstExprBuilder().appendColumnRef((name)).build()
#define EQ(expr1, expr2) buildFunction({(expr1), (expr2)}, "equals")
#define NOT_EQ(expr1, expr2) buildFunction({(expr1), (expr2)}, "notEquals")
#define lt(expr1, expr2) buildFunction({(expr1), (expr2)}, "less")
#define gt(expr1, expr2) buildFunction({(expr1), (expr2)}, "greater")
#define And(expr1, expr2) buildFunction({(expr1), (expr2)}, "and")
#define Or(expr1, expr2) buildFunction({(expr1), (expr2)}, "or")
#define NOT(expr) buildFunction({expr1}, "not")

} // namespace DB
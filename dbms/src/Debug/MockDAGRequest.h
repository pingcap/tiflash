#pragma once

#include <Debug/MockExecutor.h>
#include <Interpreters/Context.h>

#include <initializer_list>
#include <utility>

namespace DB
{
// <name, type>
using MockColumnInfo = std::pair<String, TiDB::TP>;
using MockTableName = std::pair<String, String>;
class AstExprBuilder;
class TiPBDAGRequestBuilder
{
public:
    static size_t & getExecutorIndex()
    {
        thread_local size_t executor_index = 0;
        return executor_index;
    }

    std::shared_ptr<tipb::DAGRequest> build(Context & context);

    TiPBDAGRequestBuilder & mockTable(String db, String table, const std::vector<MockColumnInfo> & columns);
    TiPBDAGRequestBuilder & mockTable(MockTableName name, const std::vector<MockColumnInfo> & columns);


    TiPBDAGRequestBuilder & filter(AstExprBuilder filter_expr);
    TiPBDAGRequestBuilder & filter(ASTPtr filter_expr);

    TiPBDAGRequestBuilder & limit(int limit);

    TiPBDAGRequestBuilder & topN(ASTPtr order_exprs, ASTPtr limit_expr);
    TiPBDAGRequestBuilder & topN(String col_name, bool desc, int limit);
    TiPBDAGRequestBuilder & topN(std::initializer_list<String> cols, bool desc, int limit); // ywq todo, support expressions.

    TiPBDAGRequestBuilder & project(String col_name); // todo support expression
    TiPBDAGRequestBuilder & project(std::initializer_list<ASTPtr> cols);
    TiPBDAGRequestBuilder & project(std::initializer_list<String> cols);

    // only support inner join  todo support more joins
    TiPBDAGRequestBuilder & join(const TiPBDAGRequestBuilder & right, ASTPtr using_expr_list);

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
    AstExprBuilder & eq(AstExprBuilder & right_expr);
    AstExprBuilder & notEq(AstExprBuilder & right_expr);
    AstExprBuilder & lt(AstExprBuilder & right_expr);
    AstExprBuilder & gt(AstExprBuilder & right_expr);
    AstExprBuilder & andFunc(AstExprBuilder & right_expr);
    AstExprBuilder & orFunc(AstExprBuilder & right_expr);

    ASTPtr build();
    ASTPtr buildEqualFunction(const String & column_left, const String & column_right);
    ASTPtr buildEqualFunction(const String & column_left, const Field & literal);

private:
    std::vector<ASTPtr> vec;
};


#define EQUALFUNCTION(col1, col2) AstExprBuilder().buildEqualFunction((col1), (col2))
#define COL(name) AstExprBuilder().appendColumnRef((name))

} // namespace DB
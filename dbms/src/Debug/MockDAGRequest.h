#pragma once

#include <Debug/MockExecutor.h>

namespace DB
{
// <name, type>
using MockColumnInfo = std::pair<String, TiDB::TP>;
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

    TiPBDAGRequestBuilder & filter(ASTPtr filter_expr);

    TiPBDAGRequestBuilder & limit(ASTPtr limit_expr);

    TiPBDAGRequestBuilder & topN(ASTPtr order_exprs, ASTPtr limit_expr);

    TiPBDAGRequestBuilder & project(ASTPtr select_list);

    // only support inner join
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

    AstExprBuilder & appendFunction(const String & func_name);

    ASTPtr build();

private:
    std::vector<ASTPtr> vec;
};
} // namespace DB
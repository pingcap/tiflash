#include <Common/FmtUtils.h>
#include <Debug/MockDAGRequest.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{
namespace
{
String toTreeString(const tipb::Executor & root_executor, size_t level = 0);

String toTreeString(const tipb::DAGRequest * dag_request)
{
    assert((dag_request->executors_size() > 0) != dag_request->has_root_executor());
    if (dag_request->has_root_executor())
    {
        return toTreeString(dag_request->root_executor());
    }
    else
    {
        FmtBuffer buffer;
        String prefix;
        traverseExecutors(dag_request, [&buffer, &prefix](const tipb::Executor & executor) {
            assert(executor.has_executor_id());
            buffer.fmtAppend("{}{}\n", prefix, executor.executor_id());
            prefix.append(" ");
            return true;
        });
        return buffer.toString();
    }
}

String toTreeString(const tipb::Executor & root_executor, size_t level)
{
    FmtBuffer buffer;

    auto append_str = [&buffer, &level](const tipb::Executor & executor) {
        assert(executor.has_executor_id());
        for (size_t i = 0; i < level; ++i)
            buffer.append(" ");
        buffer.append(executor.executor_id()).append("\n");
    };

    traverseExecutorTree(root_executor, [&](const tipb::Executor & executor) {
        if (executor.has_join())
        {
            for (const auto & child : executor.join().children())
                buffer.append(toTreeString(child, level));
            return false;
        }
        else
        {
            append_str(executor);
            ++level;
            return true;
        }
    });

    return buffer.toString();
}

String & trim(String & str)
{
    if (str.empty())
    {
        return str;
    }

    str.erase(0, str.find_first_not_of(' '));
    str.erase(str.find_last_not_of(' ') + 1);
    return str;
}
} // namespace

class MockTiPBDAGRequestTest : public DB::tests::FunctionTest
{
};

TEST_F(MockTiPBDAGRequestTest, Test)
try
{
    String empty_str;

    std::vector<MockColumnInfo> r_columns;
    r_columns.emplace_back("r_a", TiDB::TP::TypeString);
    TiPBDAGRequestBuilder right_builder;
    right_builder
        .mockTable("r_db", "r_table", r_columns)
        .filter(AstExprBuilder().appendColumnRef("r_a").appendLiteral(Field(empty_str)).appendFunction("equals").build())
        .project(AstExprBuilder().appendColumnRef("r_a").appendList().build())
        .topN(AstExprBuilder().appendOrderByItem("r_a", false).appendList().build(), AstExprBuilder().appendLiteral(Field(static_cast<UInt64>(20))).build());

    std::vector<MockColumnInfo> l_columns;
    l_columns.emplace_back("l_a", TiDB::TP::TypeString);
    TiPBDAGRequestBuilder left_builder;
    left_builder
        .mockTable("l_db", "l_table", l_columns)
        .filter(AstExprBuilder().appendColumnRef("l_a").appendLiteral(Field(empty_str)).appendFunction("equals").build())
        .project(AstExprBuilder().appendColumnRef("l_a").appendList().build())
        .join(right_builder, AstExprBuilder().appendColumnRef("l_a").appendColumnRef("r_a").appendList().build())
        .limit(AstExprBuilder().appendLiteral(Field(static_cast<UInt64>(10))).build());

    auto dag_request = left_builder.build(context);

    String to_tree_string = toTreeString(dag_request.get());
    String expect_tree_string = "limit_8\n"
                                " project_6\n"
                                "  selection_5\n"
                                "   table_scan_4\n"
                                " topn_3\n"
                                "  project_2\n"
                                "   selection_1\n"
                                "    table_scan_0\n";
    assert(trim(to_tree_string) == trim(expect_tree_string));
}
CATCH

} // namespace DB::tests
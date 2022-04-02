#include <Common/FmtUtils.h>
#include <Debug/MockDAGRequest.h>
#include <Debug/SerializeExecutor.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InterpreterTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>


namespace DB
{
namespace tests
{
class MockTiPBDAGRequestTest : public DB::tests::InterpreterTest
{
};

TEST_F(MockTiPBDAGRequestTest, Test)
try
{
    String empty_str;

    MockTableName right_table{"r_db", "r_table"};
    std::vector<MockColumnInfo> r_columns;
    r_columns.emplace_back("l_a", TiDB::TP::TypeString);
    r_columns.emplace_back("l_b", TiDB::TP::TypeString);

    MockTableName left_table{"l_db", "l_table"};
    std::vector<MockColumnInfo> l_columns;
    l_columns.emplace_back("l_a", TiDB::TP::TypeString);
    l_columns.emplace_back("l_b", TiDB::TP::TypeLong);

    TiPBDAGRequestBuilder right_builder;
    right_builder
        .mockTable(right_table, r_columns)
        .filter(EQ(COL("l_a"), COL("l_b")))
        .project({COL("l_a")})
        .topN("l_a", false, 20);


    TiPBDAGRequestBuilder left_builder;
    left_builder
        .mockTable(left_table, l_columns)
        .topN({"l_a", "l_b"}, false, 10)
        .join(right_builder, AstExprBuilder().appendColumnRef("l_a").appendList().build()) // ywq todo more types of join, should make sure have both field
        .limit(10);


    auto dag_request = left_builder.build(context);
    dagRequestEqual(dag_request, dag_request);
    String expect_tree_string = "limit_9\n"
                                " project_7\n"
                                "  selection_6\n"
                                "   table_scan_5 columns: { column_id: -1, [String]}\n"
                                " topn_4\n"
                                "  project_3\n"
                                "   selection_2\n"
                                "    selection_1\n"
                                "     table_scan_0 columns: { column_id: -1, [String]}\n";
    ASSERT_TRUE(dagRequestEqual(expect_tree_string, dag_request));
    writeResult("test_interpreter.txt");
}
CATCH

TEST_F(MockTiPBDAGRequestTest, Interpreter)
try
{
    String empty_str;

    MockTableName right_table{"r_db", "r_table"};
    std::vector<MockColumnInfo> r_columns;
    r_columns.emplace_back("l_a", TiDB::TP::TypeString);
    r_columns.emplace_back("l_b", TiDB::TP::TypeString);

    MockTableName left_table{"l_db", "l_table"};
    std::vector<MockColumnInfo> l_columns;
    l_columns.emplace_back("l_a", TiDB::TP::TypeString);
    l_columns.emplace_back("l_b", TiDB::TP::TypeLong);

    TiPBDAGRequestBuilder right_builder;
    right_builder
        .mockTable(right_table, r_columns)
        .filter(EQ(COL("l_a"), COL("l_b")))
        .project({COL("l_a")})
        .topN("l_a", false, 20);


    TiPBDAGRequestBuilder left_builder;
    left_builder
        .mockTable(left_table, l_columns)
        .topN({"l_a", "l_b"}, false, 10)
        .join(right_builder, AstExprBuilder().appendColumnRef("l_a").appendList().build()) // ywq todo more types of join, should make sure have both field
        .limit(10);

    auto dag_request = left_builder.build(context);

    DAGContext dag_context(*dag_request);
    context.setDAGContext(&dag_context);
    // Don't care about regions information in this test
    DAGQuerySource dag(context);
    executeQuery(dag, context, false, QueryProcessingStage::Complete);
    writeResult("test_interpreter.txt");
}
CATCH


} // namespace tests
} // namespace DB
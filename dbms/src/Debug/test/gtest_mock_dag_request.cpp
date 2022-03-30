#include <Common/FmtUtils.h>
#include <Debug/MockDAGRequest.h>
#include <Debug/SerializeExecutor.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include "Common/Decimal.h"

namespace DB
{
namespace tests
{
namespace
{
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
    // ywq todo add a literal method to construct literal Field.
    // expression alias
    // support wildcard..
    // more types of join..
    // support join condition construct...
    // add sum function
    // add plus minus etc
    String empty_str;

    MockTableName right_table{"r_db", "r_table"};
    std::vector<MockColumnInfo> r_columns;
    r_columns.emplace_back("r_a", TiDB::TP::TypeString);
    r_columns.emplace_back("r_b", TiDB::TP::TypeString);

    MockTableName left_table{"l_db", "l_table"};
    std::vector<MockColumnInfo> l_columns;
    l_columns.emplace_back("l_a", TiDB::TP::TypeString);
    l_columns.emplace_back("l_b", TiDB::TP::TypeLong);

    TiPBDAGRequestBuilder right_builder;
    right_builder
        .mockTable(right_table, r_columns)
        .filter(COL("r_a").eq(COL("r_b")))
        .filter(EQUALFUNCTION("r_a", "r_a"))
        .project({"r_a", "r_b"})
        .topN("r_a", false, 20);


    TiPBDAGRequestBuilder left_builder;
    left_builder
        .mockTable(left_table, l_columns)
        .filter(EQUALFUNCTION("l_a", Field(empty_str))) // ywq todo add and/or ..
        .filter(EQUALFUNCTION("l_b", makeField(10000000000000))) // illegal check...
        // .project({COL("l_a"), COL("l_b")})  // todo add function
        .topN({"l_a", "l_b"}, false, 10)
        .join(right_builder, AstExprBuilder().appendColumnRef("l_a").appendColumnRef("r_a").appendList().build()) // ywq todo more types of join
        .limit(10);

    FmtBuffer fmt_buf; // move into test context
    auto dag_request = left_builder.build(context);
    auto serialize = SerializeExecutor(context, fmt_buf);
    String to_tree_string = serialize.serialize(dag_request.get());
    std::cout << to_tree_string << std::endl;

    String expect_tree_string = "limit_9\n"
                                " project_7\n"
                                "  selection_6\n"
                                "   table_scan_5 columns: { column_id: -1, [String]}\n"
                                " topn_4\n"
                                "  project_3\n"
                                "   selection_2\n"
                                "    selection_1\n"
                                "     table_scan_0 columns: { column_id: -1, [String]}\n";
    assert(trim(to_tree_string) == trim(expect_tree_string));
}
CATCH


} // namespace tests
} // namespace DB
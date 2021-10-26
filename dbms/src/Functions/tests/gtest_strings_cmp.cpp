#include <TestUtils/FunctionTestUtils.h>

namespace DB
{
namespace tests
{
class Strcmp : public DB::tests::FunctionTest
{
};

TEST_F(Strcmp, Strcmp)
try {
    auto test_cases = TestCasesConstructor<Int8, String, String>(
            {}
            , {{-1, {"a", "b"}}, {1, {"b", "a"}}, {0, {"a", "a"}}
            });
    auto [expected, input] = test_cases.getResultAndInput();
    auto result = executeFunction("strcmp", input);
    ASSERT_COLUMN_EQ(expected, result);
}
CATCH
}
}